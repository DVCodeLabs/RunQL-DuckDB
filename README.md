# RunQL DuckDB Connector

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![VS Code](https://img.shields.io/badge/vscode-%5E1.96.0-007ACC)](https://code.visualstudio.com/)

Optional DuckDB connector for the [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) VS Code extension. Install alongside RunQL to query DuckDB databases — local `.duckdb` files, in-memory instances, or MotherDuck (`md:`) connection strings — using RunQL's SQL workflows, results panel, schema introspection, and ERD tooling.

This extension is maintained separately from the RunQL core so that users who do not need DuckDB are not required to install its native binaries.

## Installation

1. Install [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) (required — declared as an extension dependency).
2. Install **RunQL DuckDB Connector** from the VS Code Marketplace.

RunQL will detect the connector on activation and register DuckDB as an available connection provider.

## Usage

1. Open the RunQL explorer view.
2. Click **Add Connection** and choose **DuckDB**.
3. Provide either:
   - A local file path (e.g. `/path/to/my.duckdb`), or
   - A MotherDuck connection string (e.g. `md:my_database`), or
   - Leave blank / use `:memory:` for an in-memory database.
4. Save and test the connection.

DuckDB connections support schema introspection, query cancellation, and optimized CSV export via `COPY`.

## Requirements

- VS Code `^1.96.0`
- [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) extension

## How it works

On activation, this extension acquires the RunQL extension API and calls:

- `registerProvider(duckdbProvider)` — adds a DuckDB entry to the connection form.
- `registerAdapter('duckdb', () => new DuckDBAdapter())` — wires the dialect to its implementation.

The adapter uses [`@duckdb/node-api`](https://www.npmjs.com/package/@duckdb/node-api), DuckDB Labs' official async Node bindings. It ships prebuilt native binaries per platform, which is why the connector is published as platform-specific VSIXs (Windows, macOS Intel/Apple Silicon, Linux x64/ARM64) rather than a universal package.

## Building from source

```bash
git clone https://github.com/DVCodeLabs/RunQL-DuckDB.git
cd RunQL-DuckDB
npm install
npm run package   # produces dist/extension.js
```

To produce a local VSIX for testing:

```bash
npm install -g @vscode/vsce
vsce package
```

## Contributing

Issues and pull requests welcome at [DVCodeLabs/RunQL-DuckDB](https://github.com/DVCodeLabs/RunQL-DuckDB). Please follow the code of conduct shared with the RunQL project.

## License

MIT — see [LICENSE](./LICENSE).
