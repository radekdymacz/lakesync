/** Configuration for the compaction process */
export interface CompactionConfig {
	/** Minimum number of delta files before compaction triggers */
	minDeltaFiles: number;
	/** Maximum number of delta files to compact in one pass */
	maxDeltaFiles: number;
	/** Target base file size in bytes */
	targetFileSizeBytes: number;
}

/** Default compaction configuration values */
export const DEFAULT_COMPACTION_CONFIG: CompactionConfig = {
	minDeltaFiles: 10,
	maxDeltaFiles: 20,
	targetFileSizeBytes: 128 * 1024 * 1024, // 128 MB
};

/** Result of a compaction operation */
export interface CompactionResult {
	/** Number of base data files written */
	baseFilesWritten: number;
	/** Number of equality delete files written */
	deleteFilesWritten: number;
	/** Number of delta files that were compacted */
	deltaFilesCompacted: number;
	/** Total bytes read during compaction */
	bytesRead: number;
	/** Total bytes written during compaction */
	bytesWritten: number;
}
