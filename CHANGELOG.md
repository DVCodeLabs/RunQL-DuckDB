# Changelog

All notable changes to the RunQL DuckDB Connector will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — Unreleased

### Added
- Initial release, extracted from RunQL core.
- DuckDB connection provider (local file, `:memory:`, and MotherDuck `md:` strings).
- `DuckDBAdapter` with query execution, non-query execution, schema introspection, and optimized CSV export via `COPY`.
- Connection caching and graceful shutdown on extension deactivation.
- Built on [`@duckdb/node-api`](https://www.npmjs.com/package/@duckdb/node-api), the official async Node bindings with prebuilt binaries (no `node-gyp` toolchain at install time).
