import { existsSync, mkdirSync, readdirSync, statSync } from 'node:fs'
import path from 'node:path'
import {
	type MapEntry,
	type Page,
	openOrCreatePageFile,
	readPageFile,
	readValue,
	writeValue,
} from './internal/index.js'
import {
	calculateStaleBytes,
	getfreePage,
	mergePageMaps,
	removeDeletedEntires,
	updateStaleBytes,
} from './internal/pages.js'

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
	 * page file reaches this size, a new page file is created.
	 */
	maxPageSize: number

	/**
	 * The delay in milliseconds before the data is synced to disk. The default is
	 * 15s. This is useful for reducing the number of disk writes. The data is
	 * still written to the file handle but the file handle is not flushed to disk.
	 * Set it to -1 to sync after every write. A setting of -1 will drastically reduce
	 * the performance of the database but will provide maximum relaibility. In case
	 * of a crash, the data will be lost if the data is not synced to disk.
	 */
	dataSyncDelay: number

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
export class JsDb {
	private map: Map<string, MapEntry> = new Map()
	deletePage: Page | undefined
	private pages: Page[] = []
	private opened = false
	private lastUsedPage = -1
	private sequence = 0
	private opts: JsDbOptions = {
		dirPath: '',
		maxPageSize: 4096,
		dataSyncDelay: 1000 * 15,
		staleDataThreshold: 0.4,
		cachedFields: [],
	}

	/**
	 * Get the options for the database. The options are read only and cannot be changed.
	 * @returns The options for the database
	 */
	get options(): Readonly<JsDbOptions> {
		return this.opts
	}

	/**
	 * Create a new instance of the JsDb.
	 * @param options The options for the database.
	 * @throws Error if the dirPath is not provided
	 * @throws Error if the dirPath is not a directory
	 * @throws Error if the maxPageSize is less than 1024 KB (default 4096 KB)
	 * @throws Error if the staleDataThreshold is not between 0 and 1 (default 0.4)
	 */
	constructor(options: Partial<JsDbOptions>) {
		if (!options.dirPath) {
			throw new Error('dirPath is required')
		}
		this.opts.dirPath = options.dirPath

		if (options.maxPageSize) {
			if (options.maxPageSize < 1024 || options.maxPageSize % 1024 !== 0) {
				throw new Error('maxPageSize must be at least 1024 KB')
			}
			this.opts.maxPageSize = options.maxPageSize
		}

		if (options.dataSyncDelay) {
			this.opts.dataSyncDelay = 1000 * 15
		}

		if (options.staleDataThreshold) {
			if (options.staleDataThreshold < 0 || options.staleDataThreshold > 1) {
				throw new Error('staleDataThreshold must be between 0 and 1')
			}
			this.opts.staleDataThreshold = options.staleDataThreshold
		}

		if (options.cachedFields && Array.isArray(options.cachedFields) && options.cachedFields.length > 0) {
			this.opts.cachedFields = []
		}

		if (!existsSync(this.opts.dirPath)) {
			mkdirSync(this.opts.dirPath, { recursive: true })
		} else {
			// check if the path is a directory
			const stats = statSync(this.opts.dirPath)
			if (!stats.isDirectory()) {
				throw new Error('dirPath must be a directory')
			}
		}
	}

	/**
	 * Open the database for usage.
	 * This method must be called before any other method. If the database is already
	 * open, it will throw an error as it reopening the database multiple times can lead
	 * to data corruption. If the database does not exist, it will be created.
	 * @throws Error if the database is already open
	 */
	async open() {
		if (this.opened) {
			throw new Error('database already open', {
				cause:
					'The database is already open. Ensure that is database is only opened once as opening it multiple times can lead to data corruption.',
			})
		}

		this.map = new Map()

		// find all files with .page extension
		const files = readdirSync(this.opts.dirPath)
			.filter((file) => file.endsWith('.page') && !file.startsWith('10000'))
			.map((file) => path.join(this.opts.dirPath, file))

		const maps = await Promise.all(files.map((file) => readPageFile<MapEntry>(file, 'append', this.opts.cachedFields)))

		// merge the maps
		mergePageMaps(this.map, ...maps)

		// open the delete log
		const deletePagePath = path.join(this.opts.dirPath, '10000.page')
		const deleteMap = await readPageFile<number>(deletePagePath, 'delete')

		// apply the delete log
		removeDeletedEntires(this.map, deleteMap)

		this.deletePage = await openOrCreatePageFile(deletePagePath)

		// open each page
		for (const file of files) {
			const page = await openOrCreatePageFile(file)
			this.pages.push(page)
		}

		calculateStaleBytes(this.pages, this.map)

		// set the sequence number
		let seqNumber = 0
		for (const entry of this.map.values()) {
			seqNumber = Math.max(seqNumber, entry._seq)
		}
		//  also check the delete Map
		for (const entry of deleteMap.values()) {
			seqNumber = Math.max(seqNumber, entry)
		}
		this.sequence = seqNumber

		// finally open the database for usage
		this.opened = true
	}

	/**
	 * Close the database and release all resources. This method should be called
	 * when the database is no longer needed. If the database is not closed, the
	 * resources will not be released and the files will remain open.
	 */
	async close() {
		await Promise.all(this.pages.map((page) => page.close()))
		await this.deletePage?.close()
		this.pages = []
		this.deletePage = undefined
		this.map.clear()
		this.opened = false
	}

	/**
	 * Check if a record exists in the database.
	 * @param id Id of the record
	 * @returns true if the record exists, false otherwise
	 */
	exists(id: string): boolean {
		this.isOpen()
		this.isValidId(id)

		return this.map.has(id)
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
	async get(id: string): Promise<Idable | undefined> {
		this.isOpen()
		this.isValidId(id)

		const entry = this.map.get(id)
		if (!entry) return undefined

		const page = this.pages[entry.fileNumber]
		if (!page) return
		const value = (await readValue(page, entry.offset, entry.size)) as Idable
		if (!value.id) {
			throw new Error('id missing in value')
		}
		if (value.id !== id) {
			throw new Error('ID mismatch')
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
	async set<T extends Idable>(id: string, value: T): Promise<T> {
		this.isOpen()
		this.isValidId(id)

		value.id = id

		const page = await getfreePage(this.pages, this.opts.maxPageSize, this.lastUsedPage, this.opts.dirPath)

		// write the value to the page
		const offset = page.size
		this.sequence++
		value._seq = this.sequence
		const jsonStr = `${JSON.stringify(value)}\n`
		const bytesWritten = await writeValue(page.handle, Buffer.from(jsonStr))

		// update the page size for book keeping
		page.size += bytesWritten
		this.lastUsedPage = page.pageNo

		const entry: MapEntry = {
			fileNumber: page.pageNo,
			offset: offset,
			size: bytesWritten,
			_seq: value._seq,
		}

		updateStaleBytes(id, this.pages, this.map)
		this.map.set(id, entry)
		return value
	}

	/**
	 * Delete a record from the database.
	 * @param id Id of the record
	 * @throws Error if the database is not open
	 * @throws Error if a valid ID is not provided
	 */
	async delete(id: string) {
		this.isOpen()
		this.isValidId(id)

		const entry = this.map.get(id)
		if (!entry) return

		// write the value to the page
		const sequence = ++this.sequence
		const jsonStr = `${JSON.stringify({ id, _s: sequence })}\n`
		await writeValue(this.deletePage.handle, Buffer.from(jsonStr))

		// Mark the entry as deleted. In case of of deleted entry we are not tracking the size
		// of the new entry so out calculation will be slightly off but in the grand scheme of
		// things it should not matter much.
		updateStaleBytes(id, this.pages, this.map)
		this.map.delete(id)
	}

	// check if the database is open
	private isOpen(): asserts this is { deletePage: Page } {
		if (!this.opened) {
			throw new Error('db not open', {
				cause: 'The database is not open. Call open() before using the database.',
			})
		}
	}

	// check if a valid ID is provided
	private isValidId(id: string): asserts id is string {
		if (!id) {
			throw new Error('id is required', {
				cause: 'A valid ID is required for the operation.',
			})
		}
	}
}
