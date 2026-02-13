export type { BigQueryAdapterConfig } from "./bigquery";
export { BigQueryAdapter, BigQuerySqlDialect } from "./bigquery";
export type { CdcSourceConfig } from "./cdc/cdc-source";
export { CdcSource, convertChangesToDeltas } from "./cdc/cdc-source";
export type { CdcChangeBatch, CdcCursor, CdcDialect, CdcRawChange } from "./cdc/dialect";
export type { MsSqlCdcDialectConfig, MsSqlCdcRow } from "./cdc/mssql-dialect";
export {
	MsSqlCdcDialect,
	bufferToHex,
	compareLsn,
	deriveCaptureInstanceName,
	hexToBuffer,
	mapOperationToKind,
	parseMsSqlCdcRows,
} from "./cdc/mssql-dialect";
export type { ChangelogRow, MySqlCdcDialectConfig } from "./cdc/mysql-dialect";
export {
	MySqlCdcDialect,
	buildTriggerSql,
	extractColumnsFromJson,
	mysqlTypeToColumnType,
	parseChangelogRows,
} from "./cdc/mysql-dialect";
export type {
	PostgresCdcConfig,
	PostgresCdcDialectConfig,
	Wal2JsonChange,
	Wal2JsonPayload,
} from "./cdc/postgres-cdc";
export {
	createPostgresCdcSource,
	PostgresCdcDialect,
	PostgresCdcSource,
	parseWal2JsonChanges,
} from "./cdc/postgres-cdc";
export type { CompositeAdapterConfig, CompositeRoute } from "./composite";
export { CompositeAdapter } from "./composite";
export type { DatabaseAdapter, DatabaseAdapterConfig } from "./db-types";
export { isDatabaseAdapter, lakeSyncTypeToBigQuery } from "./db-types";
export type { AdapterFactory, AdapterFactoryRegistry } from "./factory";
export {
	createAdapterFactoryRegistry,
	createDatabaseAdapter,
	defaultAdapterFactoryRegistry,
} from "./factory";
export type { FanOutAdapterConfig } from "./fan-out";
export { FanOutAdapter } from "./fan-out";
export type { LifecycleAdapterConfig } from "./lifecycle";
export { LifecycleAdapter, migrateToTier } from "./lifecycle";
export type { Materialisable, QueryExecutor, SqlDialect } from "./materialise";
export {
	buildSchemaIndex,
	executeMaterialise,
	groupDeltasByTable,
	isMaterialisable,
	isSoftDelete,
	resolveConflictColumns,
	resolvePrimaryKey,
} from "./materialise";
export type { ParquetMaterialiserConfig } from "./materialise-parquet";
export { ParquetMaterialiser } from "./materialise-parquet";
export type { MigrateOptions, MigrateProgress, MigrateResult } from "./migrate";
export { migrateAdapter } from "./migrate";
export { MinIOAdapter } from "./minio";
export { MySQLAdapter, MySqlDialect } from "./mysql";
export { PostgresAdapter, PostgresSqlDialect } from "./postgres";
export type { QueryFn } from "./query-fn";
export { createQueryFn } from "./query-fn";
export { groupAndMerge, mergeLatestState, toCause, wrapAsync } from "./shared";
export type { AdapterConfig, LakeAdapter, ObjectInfo } from "./types";
