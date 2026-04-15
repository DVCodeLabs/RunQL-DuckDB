import * as vscode from 'vscode';
import { RunQLExtensionApi } from './types';
import { duckdbProvider } from './provider';
import { DuckDBAdapter } from './duckdbAdapter';

export async function activate(context: vscode.ExtensionContext) {
  const core = vscode.extensions.getExtension<RunQLExtensionApi>('runql.runql');
  if (!core) {
    vscode.window.showWarningMessage('RunQL DuckDB Connector requires runql.runql.');
    return;
  }

  const api = await core.activate();
  if (!api || typeof api.registerProvider !== 'function' || typeof api.registerAdapter !== 'function') {
    vscode.window.showWarningMessage('RunQL core API is unavailable. Update RunQL and try again.');
    return;
  }

  context.subscriptions.push(
    api.registerProvider(duckdbProvider),
    api.registerAdapter('duckdb', () => new DuckDBAdapter()),
    new vscode.Disposable(() => DuckDBAdapter.closeAllConnections())
  );
}

export function deactivate() {
  DuckDBAdapter.closeAllConnections();
}
