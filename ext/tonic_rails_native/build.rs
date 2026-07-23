//! Build script — compiles .proto files with tonic-build.
//!
//! Generates Rust message types + service traits from the proto files.
//! Also writes an encoded `FileDescriptorSet` binary for tonic-reflection
//! and a `services.rs` file with `named_dispatch_service!` invocations
//! for each service found in the protos.
//!
//! User apps can override the proto directory with the `PROTO_DIR` env var.

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::env::var("PROTO_DIR").unwrap_or_else(|_| "protos".to_string());
    let proto_dir_path = Path::new(&proto_dir);

    // Find all .proto files in the proto directory.
    let proto_files: Vec<PathBuf> = if proto_dir_path.exists() {
        std::fs::read_dir(proto_dir_path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "proto"))
            .collect()
    } else {
        Vec::new()
    };

    if proto_files.is_empty() {
        panic!(
            "No .proto files found in {}. Set PROTO_DIR env var or add .proto files.",
            proto_dir_path.display()
        );
    }

    let out_dir = std::env::var("OUT_DIR")?;
    let desc_path = Path::new(&out_dir).join("shop.desc");

    // Compile all proto files together (they may have cross-references).
    let proto_paths: Vec<&Path> = proto_files.iter().map(|p| p.as_path()).collect();
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(&desc_path)
        .compile_protos(&proto_paths, &[proto_dir_path])?;
    // Parse service names from proto files and generate services.rs.
    let services = parse_services(&proto_files);
    generate_services_rs(&out_dir, &services);

    for pf in &proto_files {
        println!("cargo:rerun-if-changed={}", pf.display());
    }
    Ok(())
}

/// A service extracted from a .proto file.
struct ProtoService {
    /// Fully-qualified service name, e.g. "shop.ProductService".
    full_name: String,
    /// Rust-safe identifier for the generated wrapper type.
    type_name: String,
}

/// Parse all .proto files to extract service declarations.
/// Each service gets a fully-qualified name (package.ServiceName).
fn parse_services(proto_files: &[PathBuf]) -> Vec<ProtoService> {
    let mut services = Vec::new();

    // Sort proto files for deterministic generation order
    // (read_dir order is filesystem-dependent and nondeterministic).
    let mut sorted_files: Vec<&PathBuf> = proto_files.iter().collect();
    sorted_files.sort();

    for proto_file in sorted_files {
        let content = std::fs::read_to_string(proto_file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", proto_file.display()));

        // Extract package name.
        let package = extract_package(&content)
            .unwrap_or_else(|| panic!("proto {} has no package declaration", proto_file.display()));

        // Extract service names.
        for service_name in extract_service_names(&content) {
            let full_name = format!("{package}.{service_name}");
            let type_name = sanitize_type_name(&full_name);
            services.push(ProtoService {
                full_name,
                type_name,
            });
        }
    }

    if services.is_empty() {
        panic!("No service declarations found in proto files");
    }

    // Sort by full_name for stable generated output.
    services.sort_by(|a, b| a.full_name.cmp(&b.full_name));

    // Dedup type names: PascalCasing + underscore stripping can still
    // collide (e.g. "My_Service" vs "MyService" in the same package).
    // Append a numeric suffix to any duplicates.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for svc in &mut services {
        let mut name = svc.type_name.clone();
        let mut suffix = 2;
        while seen.contains(&name) {
            name = format!("{}{}", svc.type_name, suffix);
            suffix += 1;
        }
        seen.insert(name.clone());
        svc.type_name = name;
    }

    services
}

/// Extract the package name from a .proto file.
fn extract_package(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("package ") {
            let name = rest.trim_end_matches(';').trim();
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

/// Extract service names from a .proto file using simple line-based parsing.
/// Skips //-commented lines.
fn extract_service_names(content: &str) -> Vec<String> {
    let mut services = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("service ") {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                services.push(name);
            }
        }
    }
    services
}

/// Convert a fully-qualified proto service name into a unique Rust-safe
/// type identifier.  Derives from package + service to avoid collisions
/// between same-named services in different packages.
/// E.g. "shop.ProductService" → "ShopProductService"
///      "catalog.ProductService" → "CatalogProductService"
///      "shop.api.v1.OrderService" → "ShopApiV1OrderService"
fn sanitize_type_name(full_name: &str) -> String {
    full_name
        .split('.')
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>()
}

/// Generate the services.rs file with named_dispatch_service! invocations
/// and a register_dispatch_services! macro.
fn generate_services_rs(out_dir: &str, services: &[ProtoService]) {
    let services_path = Path::new(out_dir).join("services.rs");
    let mut code = String::new();

    code.push_str("// Auto-generated by build.rs — do not edit.\n");
    code.push_str("// Generated NamedService wrappers for each service in the .proto files.\n\n");

    // Generate named_dispatch_service! invocations.
    for svc in services {
        code.push_str(&format!(
            "named_dispatch_service!({}, \"{}\");\n",
            svc.type_name, svc.full_name
        ));
    }

    // Generate register_dispatch_services! macro.
    code.push_str("\n/// Macro to register all generated services on a tonic Server.\n");
    code.push_str("/// Expands to `server.add_service(Svc1(inner.clone())).add_service(Svc2(inner.clone()))...`\n");
    code.push_str("macro_rules! register_dispatch_services {\n");
    code.push_str("    ($server:expr, $inner:expr) => {\n");
    code.push_str("        $server\n");
    for svc in services {
        code.push_str(&format!(
            "            .add_service({}($inner.clone()))\n",
            svc.type_name
        ));
    }
    code.push_str("    };\n");
    code.push_str("}\n");

    // Also generate a function to set health serving status for all services.
    code.push_str("\n/// Call set_serving for all registered services.\n");
    code.push_str(
        "async fn set_all_serving(reporter: &mut tonic_health::server::HealthReporter) {\n",
    );
    for svc in services {
        code.push_str(&format!(
            "    reporter.set_serving::<{}>().await;\n",
            svc.type_name
        ));
    }
    code.push_str("}\n\n");

    // And set_not_serving for all services.
    code.push_str("/// Call set_not_serving for all registered services.\n");
    code.push_str(
        "async fn set_all_not_serving(reporter: &mut tonic_health::server::HealthReporter) {\n",
    );
    for svc in services {
        code.push_str(&format!(
            "    reporter.set_not_serving::<{}>().await;\n",
            svc.type_name
        ));
    }
    code.push_str("}\n");

    std::fs::write(&services_path, code)
        .unwrap_or_else(|e| panic!("failed to write services.rs: {e}"));
    println!("cargo:rerun-if-changed=build.rs");
}
