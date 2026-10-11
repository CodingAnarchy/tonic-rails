# Security Policy

## Supported versions

Security fixes go into the latest minor release.

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/CodingAnarchy/tonic-rails/security/advisories/new)
rather than in public issues.

Include what you found, how to reproduce it, and the impact you expect.
You should get an acknowledgement within a few days and a fix or
mitigation plan once the report is confirmed. Credit is given in the
release notes unless you prefer otherwise.

## Hardening checklist

- Terminate TLS in tonic-rails (`tls_cert_path`, `tls_key_path`) or on a
  trusted network hop; the server logs a warning when running plaintext.
- Keep `reflection` off in production; it publishes your API schema.
- Keep `max_message_size`, `max_connections` and
  `concurrency_limit_per_connection` as small as your traffic allows.
- Authenticate in an interceptor and use `RpcError` messages that do not
  reveal internals.
