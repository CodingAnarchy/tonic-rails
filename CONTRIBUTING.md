# Contributing

Thanks for helping out. Bug reports, fixes and docs improvements are all
welcome; for larger changes, open an issue first so we can agree on the
approach.

## Setup

You need Ruby 3.3+, a [Rust toolchain](https://rustup.rs) and, for the
reflection specs, [grpcurl](https://github.com/fullstorydev/grpcurl).

```sh
bundle install
bundle exec rake compile
bundle exec rake          # Ruby specs and Rust unit tests
```

## Before opening a pull request

```sh
bundle exec rubocop
bundle exec rake cargo:lint   # rustfmt + clippy -D warnings
bundle exec rake
```

- Add specs for behaviour changes. Integration specs in
  `spec/integration` start a real server and call it with the `grpc` gem.
- Note user-visible changes under "Unreleased" in `CHANGELOG.md`.
- If you touch the dispatch path, run the benchmarks (`bench/run_bench.sh`)
  before and after, and mention the results.

## Layout

| Path | What lives there |
|------|------------------|
| `lib/tonic/rails` | Ruby API: handlers (`base.rb`), router, server lifecycle, Railtie |
| `ext/tonic_rails_native/src` | Rust extension: `server.rs` (lifecycle), `dispatch.rs` (request path), `gvl.rs` (Ruby bridge), `framing.rs`, `body.rs`, `net.rs` |
| `spec` | Unit, Railtie and integration specs |
| `example` | Runnable Rails app |
| `bench` | Ruby vs Rust benchmark suite |

## Releasing

1. Update `lib/tonic/rails/version.rb` and move "Unreleased" entries in
   `CHANGELOG.md` under the new version.
2. Merge to `main`, then tag: `git tag v0.2.0 && git push origin v0.2.0`.
3. The Release workflow builds the source and precompiled gems, tests
   them, publishes to RubyGems and creates the GitHub release.
