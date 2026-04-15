import { DPProviderDescriptor } from './types';

export const duckdbProvider: DPProviderDescriptor = {
  providerId: 'duckdb',
  displayName: 'DuckDB',
  dialect: 'duckdb',
  formSchema: {
    fields: [
      {
        key: 'filePath',
        label: 'Database Path or Connection String',
        type: 'file',
        tab: 'connection',
        storage: 'profile',
        required: true,
        placeholder: '/path/to/my.duckdb or md:my_database',
        description: 'Use a local file path or a MotherDuck-style connection string such as md:.',
        picker: {
          mode: 'open',
          title: 'Select DuckDB Database',
          openLabel: 'Select Database',
          canSelectFiles: true,
          canSelectFolders: false,
          filters: {
            'DuckDB Files': ['db', 'duckdb', 'ddb'],
            'All Files': ['*']
          }
        }
      }
    ],
    reuse: { disabled: true }
  },
  supports: { ssl: false, oauth: false, keypair: false, introspection: true, cancellation: true }
};
