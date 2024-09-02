import {
	type DbState,
	createOptions,
	createPageGroup,
	readValue,
} from './internal/index.js'
import { ensureDir } from './internal/io.js'
import {
	appendToFreePage,
	maxSequenceNo,
	removeDeletedEntires,
	updateStaleBytes,
} from './internal/pagegroup.js'

/**
 * Base interface for a record in the database. Essentially, a record
 * should have a unique identifier (id) field.
 */
export interface Idable {
	/**
	 * The unique identifier for the record.
	 */
	id: string

	/**
	 * The sequence number for the record. This is used to determine
	 * the order of the operations in the database. This field is auto
	 * generated and should not be set by the user.
	 */
	_seq?: number
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
	 * is a tradeoff between memory usage and speed. If the fields are not
	 * cached, the data will be read from the file every time it is accessed.
	 * The cached fields are also written twice to the file, once in the index
	 * and once in the page file.
	 */
	cachedFields: string[]

	/**
	 * The suggested size of a page file in KB. The default is 4096 KB. When the
	 * page file reaches this size, a new page file is created. This is a soft
	 * limit and the page file can grow beyond this size. The setting is ignored
	 * if the maxPageCount is reached.
	 */
	maxPageSize: number

	/**
	 * The delay in milliseconds before the data is synced to disk. The default is
	 * 1000 ms. This is useful for reducing the number of disk writes. The data is
	 * still written to the file handle but the file handle is not flushed to disk.
	 * Set it to -1 to sync after every write. A setting of 0 will reduce the
	 * performance of the database but will provide maximum relaibility. In case
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
	 * The delay in milliseconds before the page is compacted. The default is 5
	 * minutes. The page is eligible for compaction when the size of the stale
	 * entries reaches the staleDataThreshold percentage.
	 * Compaction is done asynchronously and does not block the main thread. There
	 * should still be an impact of around 50ms when the pages are switched in
	 * the main thread.
	 */
	comapctDelay: number

	/**
	 * The threshold in bytes for stale data in a page. When the stale data reaches
	 * this threshold, the page is considered is compacted to free up the space. The
	 * default is 0.4. Setting the value to 0 will disable compaction. Setting the
	 * value low will cause frequent compaction and reduce the performance of the
	 * database.
	 */
	staleDataThreshold: number
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
	get(id: string): Promise<Idable | undefined>

	/**
	 * Set a record in the database.
	 * @param id Id of the record
	 * @param value The value to store
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	set(id: string, value: Idable): Promise<Idable>

	/**
	 * Delete a record from the database.
	 * @param id Id of the record
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	delete(id: string): Promise<void>
}

/**
 * Create a new instance of the JsDb.
 * @param options The options for the database.
 * @throws Error if the dirPath is not provided
 * @throws Error if the dirPath is not a directory
 * @throws Error if the maxPageSize is less than 1024 KB (default 4096 KB)
 * @throws Error if the staleDataThreshold is not between 0 and 1 (default 0.4)
 */
export async function openDb(
	options: Partial<JsDbOptions>,
): Promise<Readonly<JsDb>> {
	const opts = createOptions(options)

	ensureDir(opts.dirPath)

	const data = await createPageGroup(opts.dirPath, '.page', opts)
	const logs = await createPageGroup(opts.dirPath, '.log', opts)

	// apply the delete log
	removeDeletedEntires(data.map, logs.map)

	// calculateStaleBytes(pages, map)

	// set the sequence number
	const seqNo = Math.max(maxSequenceNo(data), maxSequenceNo(logs))

	const state: DbState = {
		opts,
		data,
		logs,
		seqNo,
		timers: [],
		opened: true,
	}

	// create various timers
	// // commit the data to disk periodically
	// if (opts.dataSyncDelay !== -1) {
	// 	state.timers.push(
	// 		setInterval(async () => {
	// 			await Promise.all(pages.map((page) => page.handle.datasync()))
	// 			await deletePage?.handle.datasync()
	// 		}, opts.dataSyncDelay),
	// 	)
	// }

	// // compact the pages periodically
	// state.timers.push(
	// 	setInterval(() => {
	// 		const page = pages.find(
	// 			(page) => page.size > opts.maxPageSize * opts.staleDataThreshold,
	// 		)
	// 		if (page) {
	// 			compactPage(map, pages, page)
	// 		}
	// 	}, opts.comapctDelay),
	// )

	return {
		close: close.bind(null, state),
		has: hasItem.bind(null, state),
		get: getItem.bind(null, state),
		set: setItem.bind(null, state),
		delete: deleteItem.bind(null, state),
	}
}

// check if a valid ID is provided
function isValidId(id: string): asserts id is string {
	if (!id) {
		throw new Error('id is required')
	}
}

// check if a valid ID is provided
function dbIsOpen(state: DbState) {
	if (!state.opened) {
		throw new Error(
			'Db should be opened before operation.' +
				'Make sure you are not using a closed instance of Db.',
		)
	}
}

/**
 * Close the database and release all resources. This method should be called
 * when the database is no longer needed. If the database is not closed, the
 * resources will not be released and the files will remain open.
 */
async function close(state: DbState): Promise<void> {
	dbIsOpen(state)

	// clear all the timers to avoid memory leaks
	for (const timer of state.timers) {
		clearInterval(timer)
	}

	await Promise.all([state.data.close(), state.logs.close()])
	// reset the state
	state.opened = false
}

/**
 * Check if a record exists in the database. It is faster than get as no file
 * read is required.
 * @param id Id of the record
 * @returns true if the record exists, false otherwise
 * @throws Error if the database is not open
 * @throws Error if a valid ID is not provided
 */
function hasItem(state: DbState, id: string): boolean {
	isValidId(id)
	dbIsOpen(state)

	return state.data.map.has(id)
}

/**
 * Get a record from the database.
 * @param id Id of the record
 * @returns The record if it exists, undefined otherwise
 * @throws Error if the database is not open
 * @throws Error if a valid ID is not provided
 * @throws Error if the ID is missing in the record
 * @throws Error if the ID does not match the ID saved in the record
 */
async function getItem(
	state: DbState,
	id: string,
): Promise<Idable | undefined> {
	isValidId(id)
	dbIsOpen(state)

	const entry = state.data.map.get(id)
	if (!entry) return undefined

	const page = state.data.pages.find((page) => page.pageId === entry.pageId)
	if (!page) return
	const value = (await readValue(page, entry.offset, entry.size)) as Idable
	if (!value.id) {
		throw new Error('INTERNAL: id missing in value')
	}
	if (value.id !== id) {
		throw new Error('INTERNAL: id mismatch')
	}
	return value
}

/**
 * Set a record in the database.
 * @param id Id of the record
 * @param value The value to store
 * @throws Error if the database is not open
 * @throws Error if a valid ID is not provided
 */
async function setItem(
	state: DbState,
	id: string,
	value: Idable,
): Promise<Idable> {
	isValidId(id)
	dbIsOpen(state)

	value.id = id
	const seqNo = ++state.seqNo

	const entry = await appendToFreePage(state.data, seqNo, value)
	updateStaleBytes(id, state.data.pages, state.data.map)

	// TODO: set the cached fields on the entry
	state.data.map.set(id, entry)
	return value
}

/**
 * Delete a record from the database.
 * @param id Id of the record
 * @throws Error if the database is not open
 * @throws Error if a valid ID is not provided
 */
async function deleteItem(state: DbState, id: string): Promise<void> {
	isValidId(id)
	dbIsOpen(state)

	const entry = state.data.map.get(id)
	if (!entry) return

	const seqNo = ++state.seqNo
	const delEntry = await appendToFreePage(state.logs, seqNo, {
		id,
		_seq: seqNo,
	})
	state.logs.map.set(id, delEntry)
	updateStaleBytes(id, state.data.pages, state.data.map)
	state.data.map.delete(id)
}
