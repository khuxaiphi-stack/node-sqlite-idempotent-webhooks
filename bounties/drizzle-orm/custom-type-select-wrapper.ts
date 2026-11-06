/**
 * Custom SQL Selector Wrapper (`selectFromDb`) Support for `customType` in Drizzle ORM
 * Bounty: drizzle-team/drizzle-orm #1083 ($30 USD)
 */
export interface CustomTypeValues<TData = unknown, TDriverParam = unknown> {
  data: TData;
  driverParam: TDriverParam;
  config?: Record<string, unknown>;
}

export interface CustomTypeParams<T extends CustomTypeValues> {
  dataType: (config?: T['config']) => string;
  toDriver?: (value: T['data']) => T['driverParam'];
  fromDriver?: (value: T['driverParam']) => T['data'];
  selectFromDb?: (columnSqlIdentifier: string, config?: T['config']) => string;
}

export class CustomColumnBuilder<T extends CustomTypeValues> {
  readonly columnName: string;
  readonly config?: T['config'];
  readonly params: CustomTypeParams<T>;

  constructor(columnName: string, params: CustomTypeParams<T>, config?: T['config']) {
    this.columnName = columnName;
    this.params = params;
    this.config = config;
  }

  getSqlType(): string {
    return this.params.dataType(this.config);
  }

  buildSelectExpression(tableName?: string): string {
    const quotedCol = tableName ? `"${tableName}"."${this.columnName}"` : `"${this.columnName}"`;
    if (typeof this.params.selectFromDb === 'function') {
      return `${this.params.selectFromDb(quotedCol, this.config)} AS "${this.columnName}"`;
    }
    return quotedCol;
  }

  mapToDriverValue(value: T['data']): T['driverParam'] | T['data'] {
    return this.params.toDriver ? this.params.toDriver(value) : value;
  }

  mapFromDriverValue(raw: T['driverParam']): T['data'] | T['driverParam'] {
    return this.params.fromDriver ? this.params.fromDriver(raw) : raw;
  }
}

export function customType<T extends CustomTypeValues = CustomTypeValues>(params: CustomTypeParams<T>) {
  return (columnName: string, fieldConfig?: T['config']) =>
    new CustomColumnBuilder<T>(columnName, params, fieldConfig);
}
