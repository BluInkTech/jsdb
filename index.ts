import {
	closeDb,
	createDbState,
	createOptions,
	deleteItem,
	getItem,
	hasItem,
	setItem,
} from './internal/state.js'

export type Id = string | number

/**
 * Base interface for a record in the database.
 */
export interface Idable {
	/**
	 * The unique identifier for the record.
	 */
	id: Id

	/**
	 * Internal unique identifier for the record. This is used internally by
	 * indexes and capturing relationships between records. Being a number,
	 * it is memory efficient and provides faster comparison.
	 */
	_rid: number

	/**
	 * Represents the type of operation. This is used internally to determine
	 * the type of operation that should be performed on the record. The value
	 * is set by the database and should not be set by the user.
	 * 1 - Set record
	 * 2 - Delete record
	 * 3 - Set relationship
	 * 4 - Delete relationship
	 */
	_oid: number

	/**
	 * The sequence number for the record. This is used to determine
	 * the order of the operations in the database. This field is auto
	 * generated and should not be set by the user.
	 */
	_seq: number
	[index: string]: unknown
}

/**
 * The options for the JsDb.
 */
export type JsDbOptions = {
	/**
	 * The directory where the database files are stored. There should
	 * be a separate directory for each database.
	 */
	dirPath: string

	/**
	 * The fields that should be cached in memory for faster access. There
	 * is a trade off between memory usage and speed. If the fields are not
	 * cached, the data will be read from the file every time it is accessed.
	 * The cached fields are also written twice to the file, once in the index
	 * and once in the block file.
	 */
	cachedFields: string[]

	/**
	 * The suggested size of a block file in KB. The default is 4096 KB. When the
	 * block file reaches this size, a new block file is created. This is a soft
	 * limit and the block file can grow beyond this size.
	 */
	maxBlockSize: number

	/**
	 * The delay in milliseconds before the data is synced to disk. The default is
	 * 1000 ms. This is useful for reducing the number of disk writes. The data is
	 * still written to the file handle but the file handle is not flushed to disk.
	 * Set it to -1 to sync after every write. A setting of 0 will reduce the
	 * performance of the database but will provide maximum reliability. In case
	 * of a crash, the data will be lost if the data is not synced to disk.
	 *
	 * The delay value is a soft limit and it is possible for the data to be synced
	 * to disk before or after the delay is reached. In case the Db is under heavy
	 * load, the data will be synced to disk as soon as possible to avoid slow down.
	 * In technical terms we use the delay as debounce value and not a throttle value.
	 * It is done to handle large burst of writes.
	 *
	 * The data is always synced to disk when the db is closed or automatically by
	 * the platform if the buffer is full.
	 */
	dataSyncDelay: number

	/**
	 * The delay in milliseconds before the block is compacted. The default is 5
	 * minutes. The block is eligible for compaction when the size of the stale
	 * entries reaches the staleDataThreshold percentage.
	 * Compaction is done asynchronously and does not block the main thread. There
	 * should still be an impact of around 50ms when the blocks are switched in
	 * the main thread.
	 */
	compactDelay: number

	/**
	 * The threshold in bytes for stale data in a block. When the stale data reaches
	 * this threshold, the block is considered is compacted to free up the space. The
	 * default is 0.4. Setting the value to 0 will disable compaction. Setting the
	 * value low will cause frequent compaction and reduce the performance of the
	 * database.
	 */
	staleDataThreshold: number

	/**
	 * A callback function that is called when an error occurs in the database.
	 * @param error The error that occurred
	 * @returns The error handler is called when an error occurs in the database.
	 */
	onError?: (error: Error) => void
}

/**
 * A KV database that stores data in JSON newline files.
 */
export interface JsDb {
	/**
	 * Close the database and release all resources. This method should be called
	 * when the database is no longer needed. If the database is not closed, the
	 * resources will not be released and the files will remain open.
	 */
	close(): Promise<void>

	/**
	 * Check if a record exists in the database. It is faster than get as no file
	 * read is required.
	 * @param id Id of the record
	 * @returns true if the record exists, false otherwise
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	has(id: string): boolean

	/**
	 * Get a record from the database.
	 * @param id Id of the record
	 * @returns The record if it exists, undefined otherwise
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 * @throws Error if the ID is missing in the record
	 * @throws Error if the ID does not match the ID saved in the record
	 */
	get(id: string): Idable | undefined

	/**
	 * Set a record in the database.
	 * @param id Id of the record
	 * @param value The value to store
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	set(id: string, value: Partial<Idable>): Idable

	/**
	 * Delete a record from the database.
	 * @param id Id of the record
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	delete(id: string): void
}

/**
 * Create a new instance of the JsDb.
 * @param options The options for the database.
 * @throws Error if the dirPath is not provided
 * @throws Error if the dirPath is not a directory
 * @throws Error if the maxBlockSize is less than 1024 KB (default 4096 KB)
 * @throws Error if the staleDataThreshold is not between 0 and 1 (default 0.4)
 */
export async function openDb(
	options: Partial<JsDbOptions>,
): Promise<Readonly<JsDb>> {
	const opts = createOptions(options)
	const state = await createDbState(opts)

	const db: JsDb = {
		close: closeDb.bind(null, state),
		has: hasItem.bind(null, state),
		get: getItem.bind(null, state),
		set: setItem.bind(null, state),
		delete: deleteItem.bind(null, state),
	}

	// Object.freeze(db)
	return db
}
