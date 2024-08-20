import { existsSync, mkdirSync, readdirSync, statSync } from 'node:fs'
import { open } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'
import path from 'node:path'
import { debug } from 'node:util'

const logger = debug('jsdb')
const MAX_PAGE_SIZE = 1024 * 1024 * 4 // 4MB

export interface Idable {
	id: string
}

type MapEntry = {
	f: number // file number
	o: number // offset
	s: number // size
	_?: unknown // inline data used for fast access
} & Idable

type Page = {
	fileName: string
	locked: boolean
	handle: FileHandle
	size: number
	staleBytes: number
	close: () => Promise<void>
}

/**
 * The options for the JsDb.
 */
export type JsDbOptions = {
	/**
	 * The directory where the database files are stored. There should
	 * be a separate directory for each database.
	 */
	readonly dirPath: string

	/**
	 * The fields that should be cached in memory for faster access. There
	 * is a tradeoff between memory usage and speed. If the fields are not
	 * cached, the data will be read from the file every time it is accessed.
	 * The cached fields are also written twice to the file, once in the index
	 * and once in the page file.
	 */
	cachedFields?: string[]

	/**
	 * The suggested size of a page file in KB. The default is 4096 KB. When the
	 * page file reaches this size, a new page file is created.
	 */
	maxPageSize?: number

	/**
	 * The delay in milliseconds before the data is synced to disk. The default is
	 * 15s. This is useful for reducing the number of disk writes. The data is
	 * still written to the file handle but the file handle is not flushed to disk.
	 * Set it to -1 to sync after every write. A setting of -1 will drastically reduce
	 * the performance of the database but will provide maximum relaibility. In case
	 * of a crash, the data will be lost if the data is not synced to disk.
	 */
	dataSyncDelay?: number

	/**
	 * The threshold in bytes for stale data in a page. When the stale data reaches
	 * this threshold, the page is considered is compacted to free up the space. The
	 * default is 0.4. Setting the value to 0 will disable compaction. Setting the
	 * value low will cause frequent compaction and reduce the performance of the
	 * database.
	 */
	staleDataThreshold?: number
}

/**
 * A KV database that stores data in JSON newline files.
 */
export class JsDb {
	private map: Map<string, MapEntry> = new Map()
	private pages: Page[] = []
	index: Page | undefined
	private lastUsedPage = -1

	/**
	 * Create a new instance of the JsDb.
	 * @param options The options for the database.
	 * @throws Error if the dirPath is not provided
	 * @throws Error if the dirPath is not a directory
	 * @throws Error if the maxPageSize is less than 1024 KB (default 4096 KB)
	 * @throws Error if the staleDataThreshold is not between 0 and 1 (default 0.4)
	 */
	constructor(readonly options: JsDbOptions) {
		if (!options.dirPath) {
			throw new Error('dirPath is required')
		}

		if (!options.maxPageSize) {
			options.maxPageSize = 4096
		} else if (options.maxPageSize < 1024 || options.maxPageSize % 1024 !== 0) {
			throw new Error('maxPageSize must be at least 1024 KB')
		}

		if (!options.dataSyncDelay) {
			options.dataSyncDelay = 1000 * 15
		}

		if (!options.staleDataThreshold) {
			options.staleDataThreshold = 0.4
		} else if (options.staleDataThreshold < 0 || options.staleDataThreshold > 1) {
			throw new Error('staleDataThreshold must be between 0 and 1')
		}

		if (!options.cachedFields) {
			options.cachedFields = []
		}

		if (!existsSync(options.dirPath)) {
			mkdirSync(options.dirPath, { recursive: true })
		} else {
			// check if the path is a directory
			const stats = statSync(options.dirPath)
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
		if (this.index) {
			throw new Error('database already open', {
				cause:
					'The database is already open. Ensure that is database is only opened once as opening it multiple times can lead to data corruption.',
			})
		}
		const indexPath = path.join(this.options.dirPath, 'index.db')
		this.index = await openIndex(indexPath, this.map)

		// find all files with .page extension
		const files = readdirSync(this.options.dirPath)
			.filter((file) => file.endsWith('.page'))
			.map((file) => path.join(this.options.dirPath, file))

		// open each page
		for (const file of files) {
			const page = await openPage(file)
			this.pages.push(page)
		}

		this.calculateStaleBytes()
	}

	/**
	 * Close the database and release all resources. This method should be called
	 * when the database is no longer needed. If the database is not closed, the
	 * resources will not be released and the files will remain open.
	 */
	async close() {
		await Promise.all(this.pages.map((page) => page.close()))
		await this.index?.close()
		this.index = undefined
		this.pages = []
		this.map.clear()
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

		const page = this.pages[entry.f]
		if (!page) return
		const value = (await readValue(page, entry.o, entry.s)) as Idable
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
	async set<T extends Idable>(id: string, value: T) {
		this.isOpen()
		this.isValidId(id)

		value.id = id
		const json = `${JSON.stringify(value)}\n`

		// find the page with the least size to store the data
		let page: Page | undefined = undefined
		for (let i = 0; i < this.pages.length; i++) {
			const p = this.pages[i]
			if (p && p.size < MAX_PAGE_SIZE && i !== this.lastUsedPage && !p.locked) {
				page = p
				this.lastUsedPage = i
				break
			}
		}

		// if no page is found, create a new one
		if (!page) {
			page = await openPage(path.join(this.options.dirPath, `${this.pages.length}.page`))
			this.pages.push(page)
			this.lastUsedPage = this.pages.length - 1
		}

		// first write the value to the page so that we can get the offset and size
		// if it fails we will not update the index
		const offset = page.size
		const bytesWritten = await writeValue(page, json)

		// update the index, the size can't be json.length as emoji characters get reported differently
		// using the length property
		const entry = { id, f: this.lastUsedPage, o: offset, s: bytesWritten }
		await writeValue(this.index, `${JSON.stringify(entry)}\n`)

		// check if the entry already exists in map
		const existingEntry = this.map.get(id)
		if (existingEntry) {
			// update the stale bytes for the page
			const existingPage = this.pages[existingEntry.f]
			if (existingPage) {
				existingPage.staleBytes += existingEntry.s
			}
		}
		this.map.set(id, entry)
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

		// mark the entry as deleted
		await writeValue(this.index, `-${id}\n`)
		this.map.delete(id)
	}

	private isOpen(): asserts this is { index: Page } {
		if (this.index === undefined) {
			throw new Error('db not open', {
				cause: 'The database is not open. Call open() before using the database',
			})
		}
	}

	// check if a valid ID is provided
	private isValidId(id: string): asserts id is string {
		if (!id) {
			throw new Error('id is required', {
				cause: 'A valid ID is required for the operation',
			})
		}
	}

	// calculate the stale bytes in a page by summing the sizes of all values in a page
	// minus the size of the page
	private calculateStaleBytes() {
		const totalData: Record<number, number> = {}
		for (const [_, entry] of this.map) {
			totalData[entry.f] = (totalData[entry.f] || 0) + entry.s
		}

		for (let i = 0; i < this.pages.length; i++) {
			const page = this.pages[i]
			if (!page) continue
			page.staleBytes = page.size - (totalData[i] || 0)
		}
	}
}

// open an index file or create a new one
async function openIndex(indexPath: string, map: Map<string, MapEntry>): Promise<Page> {
	if (existsSync(indexPath)) {
		await readJsonNlFile(indexPath, map)
	}
	return openPage(indexPath)
}

// read a jsonl file and populate the map
async function readJsonNlFile(path: string, map: Map<string, MapEntry>) {
	const handle = await open(path, 'a+')
	for await (const line of handle.readLines()) {
		if (line === '') continue

		if (line.startsWith('-')) {
			// it is a delete operation, get the id and remove it from the map
			const id = line.slice(1)
			map.delete(id)
		} else {
			const json = JSON.parse(line)
			if (!json.id) {
				continue
			}
			map.set(json.id, json)
		}
	}
	await handle.close()
}

// Page related functions
// open a page record or create a new one
async function openPage(pagePath: string): Promise<Page> {
	const handle = await open(pagePath, 'a+')
	const stats = await handle.stat()
	logger('Opened file descriptor', handle.fd, pagePath)
	return {
		fileName: pagePath,
		locked: false,
		handle,
		size: stats.size,
		staleBytes: 0,
		close: async () => {
			await handle.close()
		},
	}
}

async function readValue(page: Page, offset: number, length: number): Promise<unknown> {
	const value = await page.handle.read(Buffer.alloc(length), 0, length, offset)
	return JSON.parse(value.buffer.toString())
}

// write a value to the end of the page
async function writeValue(page: Page, value: string): Promise<number> {
	try {
		const buffer = Buffer.from(value)
		const written = await page.handle.write(buffer, 0, buffer.length, -1)
		page.size += written.bytesWritten
		// await page.handle.datasync()
		return written.bytesWritten
	} catch (error) {
		logger('Error writing to page', error)
		throw error
	}
}
