import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import {
  ConnectionProfile,
  ConnectionSecrets,
  DbAdapter,
  NonQueryResult,
  QueryColumn,
  QueryResult,
  QueryRunOptions,
  RoutineModel,
  SchemaIntrospection,
  TableModel
} from './types';

interface DuckDBSchemaEntry {
  name: string;
  tables: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys: unknown[]; indexes?: { name: string; columns: string[]; unique: boolean }[]; primaryKey?: string[] }>;
  views: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys: unknown[]; indexes?: { name: string; columns: string[]; unique: boolean }[] }>;
  procedures: RoutineModel[];
  functions: RoutineModel[];
  catalog: string;
  originalSchema: string;
}

interface DuckDBTestProfile extends ConnectionProfile {
  _runqlAllowCreateOnTest?: boolean;
}

interface CachedConnection {
  instance: DuckDBInstance;
  connection: DuckDBConnection;
}

function toJsonSafe(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'bigint') {
    return value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER
      ? Number(value)
      : value.toString();
  }
  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value.toISOString();
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('base64');
  }
  if (Array.isArray(value)) {
    return value.map(toJsonSafe);
  }
  if (typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const key in value as Record<string, unknown>) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        out[key] = toJsonSafe((value as Record<string, unknown>)[key]);
      }
    }
    return out;
  }
  return value;
}

function quoteIdentifier(id: string): string {
  return `"${id.replace(/"/g, '""')}"`;
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

export class DuckDBAdapter implements DbAdapter {
  readonly dialect = 'duckdb';

  private static connectionCache = new Map<string, CachedConnection>();

  async testConnection(profile: ConnectionProfile, _secrets: ConnectionSecrets): Promise<void> {
    const candidate = profile as DuckDBTestProfile;
    const dbPath = profile.filePath || ':memory:';
    const allowCreateOnTest = candidate._runqlAllowCreateOnTest === true;

    if (!allowCreateOnTest && this.isLocalFilePath(dbPath) && !fs.existsSync(dbPath)) {
      throw new Error(`DuckDB file not found: ${dbPath}. Click Save Connection to create it.`);
    }

    const instance = await DuckDBInstance.create(dbPath);
    const connection = await instance.connect();
    try {
      await connection.run('SELECT 1');
    } finally {
      connection.disconnectSync();
    }
  }

  async runQuery(
    profile: ConnectionProfile,
    _secrets: ConnectionSecrets,
    sql: string,
    _options: QueryRunOptions
  ): Promise<QueryResult> {
    const { connection } = await this.openConnection(profile);
    const start = Date.now();
    const reader = await connection.runAndReadAll(sql);
    const elapsedMs = Date.now() - start;

    const rawRows = reader.getRowObjectsJson();
    const rows = rawRows.map(toJsonSafe) as Record<string, unknown>[];

    const columnNames = reader.columnNames();
    const columnTypes = reader.columnTypes();
    const columns: QueryColumn[] = columnNames.map((name, i) => ({
      name,
      type: String(columnTypes[i] ?? 'unknown')
    }));

    return {
      columns,
      rows,
      rowCount: rows.length,
      elapsedMs
    };
  }

  async executeNonQuery(
    profile: ConnectionProfile,
    _secrets: ConnectionSecrets,
    sql: string
  ): Promise<NonQueryResult> {
    const { connection } = await this.openConnection(profile);
    const result = await connection.run(sql);
    const changed = result.rowsChanged;
    const affectedRows = typeof changed === 'number' ? changed : null;
    return { affectedRows };
  }

  async introspectSchema(
    profile: ConnectionProfile,
    _secrets: ConnectionSecrets
  ): Promise<SchemaIntrospection> {
    const { connection } = await this.openConnection(profile);

    const schemasMap = new Map<string, DuckDBSchemaEntry>();

    let primaryCatalog = 'memory';
    if (profile.filePath && profile.filePath !== ':memory:') {
      primaryCatalog = path.basename(profile.filePath, path.extname(profile.filePath));
    }

    const getSchemaKey = (catalog: string, schema: string) => `${catalog}.${schema}`;

    const getDisplayName = (catalog: string, schema: string) => {
      if (catalog === primaryCatalog || catalog === 'memory') return schema;
      if (schema === 'main') return catalog;
      return `${catalog}.${schema}`;
    };

    const schemaReader = await connection.runAndReadAll(
      'SELECT catalog_name, schema_name FROM information_schema.schemata ORDER BY catalog_name, schema_name'
    );
    for (const s of schemaReader.getRowObjectsJson() as Record<string, unknown>[]) {
      const key = getSchemaKey(String(s.catalog_name), String(s.schema_name));
      const displayName = getDisplayName(String(s.catalog_name), String(s.schema_name));
      schemasMap.set(key, {
        name: displayName,
        tables: new Map(),
        views: new Map(),
        procedures: [],
        functions: [],
        catalog: String(s.catalog_name),
        originalSchema: String(s.schema_name)
      });
    }

    const tableTypeReader = await connection.runAndReadAll(`
      SELECT table_catalog, table_schema, table_name, table_type
      FROM information_schema.tables
      ORDER BY table_catalog, table_schema, table_name
    `);
    const tableTypeMap = new Map<string, string>();
    for (const tt of tableTypeReader.getRowObjectsJson() as Record<string, unknown>[]) {
      const ttKey = `${tt.table_catalog}.${tt.table_schema}.${tt.table_name}`;
      tableTypeMap.set(ttKey, String(tt.table_type));
    }

    const columnReader = await connection.runAndReadAll(`
      SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
      FROM information_schema.columns
      ORDER BY table_catalog, table_schema, table_name, ordinal_position
    `);
    for (const row of columnReader.getRowObjectsJson() as Record<string, unknown>[]) {
      const cName = String(row.table_catalog);
      const sName = String(row.table_schema);
      const tName = String(row.table_name);

      const key = getSchemaKey(cName, sName);
      if (!schemasMap.has(key)) {
        schemasMap.set(key, {
          name: getDisplayName(cName, sName),
          tables: new Map(),
          views: new Map(),
          procedures: [],
          functions: [],
          catalog: cName,
          originalSchema: sName
        });
      }
      const schema = schemasMap.get(key)!;

      const typeKey = `${cName}.${sName}.${tName}`;
      const tableType = tableTypeMap.get(typeKey) || 'BASE TABLE';
      const targetMap = tableType === 'VIEW' ? schema.views : schema.tables;

      if (!targetMap.has(tName)) {
        targetMap.set(tName, { name: tName, columns: [], foreignKeys: [] });
      }
      const table = targetMap.get(tName);
      if (!table) continue;

      table.columns.push({
        name: String(row.column_name),
        type: String(row.data_type),
        nullable: row.is_nullable === 'YES'
      });
    }

    try {
      const idxReader = await connection.runAndReadAll(`
        SELECT schema_name, table_name, index_name, is_unique, sql
        FROM duckdb_indexes()
        ORDER BY schema_name, table_name, index_name
      `);
      for (const row of idxReader.getRowObjectsJson() as Record<string, unknown>[]) {
        const sName = String(row.schema_name);
        const tName = String(row.table_name);

        let schema: DuckDBSchemaEntry | undefined;
        for (const [, s] of schemasMap) {
          if (s.originalSchema === sName || s.name === sName) {
            schema = s;
            break;
          }
        }
        if (!schema) continue;
        const table = schema.tables.get(tName);
        if (!table) continue;

        const sqlStr = typeof row.sql === 'string' ? row.sql : '';
        const colMatch = sqlStr.match(/\(([^)]+)\)/);
        if (!colMatch) continue;
        const cols = colMatch[1].split(',').map((c) => c.trim().replace(/^"(.*)"$/, '$1'));

        if (!table.indexes) table.indexes = [];
        table.indexes.push({
          name: String(row.index_name),
          columns: cols,
          unique: row.is_unique === true
        });
      }
    } catch {
      // Index introspection is best-effort.
    }

    const showInternal = vscode.workspace.getConfiguration('runql').get('ui.showSystemSchemas', false);

    const schemas = Array.from(schemasMap.values())
      .filter((s) => {
        if (s.originalSchema === 'information_schema' || s.originalSchema === 'pg_catalog') return false;
        if (s.catalog === 'data_cache') return false;
        if (s.originalSchema === 'dp_app' && !showInternal) return false;
        return true;
      })
      .map((s) => ({
        name: s.name,
        tables: Array.from(s.tables.values()) as TableModel[],
        views: Array.from(s.views.values()) as TableModel[],
        procedures: s.procedures,
        functions: s.functions
      }));

    return {
      version: '0.2',
      generatedAt: new Date().toISOString(),
      connectionId: profile.id,
      connectionName: profile.name,
      dialect: 'duckdb',
      schemas
    };
  }

  async exportTable(
    profile: ConnectionProfile,
    _secrets: ConnectionSecrets,
    schema: string,
    table: string,
    format: 'csv' | 'json',
    outputUri: vscode.Uri
  ): Promise<void> {
    if (format !== 'csv') {
      throw new Error('JSON export not optimized for DuckDB yet');
    }
    const { connection } = await this.openConnection(profile);
    const fullTableName = `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
    const copySql = `COPY (SELECT * FROM ${fullTableName}) TO ${quoteLiteral(outputUri.fsPath)} (HEADER, DELIMITER ',')`;
    await connection.run(copySql);
  }

  private isLocalFilePath(dbPath: string): boolean {
    return dbPath !== ':memory:' && !dbPath.startsWith('md:');
  }

  private async openConnection(profile: ConnectionProfile): Promise<CachedConnection> {
    const cacheKey = profile.id?.trim();
    if (cacheKey && DuckDBAdapter.connectionCache.has(cacheKey)) {
      return DuckDBAdapter.connectionCache.get(cacheKey)!;
    }

    const dbPath = profile.filePath || ':memory:';
    const instance = await DuckDBInstance.create(dbPath);
    const connection = await instance.connect();
    const entry: CachedConnection = { instance, connection };

    if (cacheKey) {
      DuckDBAdapter.connectionCache.set(cacheKey, entry);
    }
    return entry;
  }

  public static closeConnection(profileId: string) {
    const entry = DuckDBAdapter.connectionCache.get(profileId);
    if (!entry) return;
    DuckDBAdapter.connectionCache.delete(profileId);
    try {
      entry.connection.disconnectSync();
    } catch (e) {
      console.warn(`[DuckDB] error closing connection ${profileId}:`, e);
    }
  }

  public static closeAllConnections() {
    for (const [id] of DuckDBAdapter.connectionCache) {
      DuckDBAdapter.closeConnection(id);
    }
  }
}
