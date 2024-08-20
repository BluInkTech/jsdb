import { createReadStream, existsSync, mkdirSync, readdirSync } from 'node:fs'
import { open } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'
import path from 'node:path'
import { createInterface } from 'node:readline'

const MAX_PAGE_SIZE = 1024 * 1024 // 1MB

export interface Idable {
	id: string
}

type MapEntry = {
	f: number // file number
	o: number // offset
	s: number // size
	_?: unknown // inline data
} & Idable

type Page = {
	fileName: string
	locked: boolean
	mutex: Mutex
	handle: FileHandle
	size: number
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
}

/**
 * Represents a mutex that provides exclusive access to a resource.
 */
class Mutex {
	private mutex = Promise.resolve()

	lock(): PromiseLike<() => void> {
		let begin: (unlock: () => void) => void = () => {}

		this.mutex = this.mutex.then(() => {
			return new Promise(begin)
		})

		return new Promise((res) => {
			begin = res
		})
	}
}

/**
 * A KV database that stores data in JSON files.
 */
export class JsDb {
	private map: Map<string, MapEntry> = new Map()
	private pages: Page[] = []
	private index?: Page
	private lastUsedPage = -1

	constructor(readonly options: JsDbOptions) {
		// check if the directory exists
		// create the directory if it does not exist
		mkdirSync(options.dirPath, { recursive: true })
	}

	// Open the database for usage.
	async open() {
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
	}

	// Close the database and release all resources.
	async close() {
		await Promise.all(this.pages.map((page) => page.handle?.close()))
		await this.index?.handle.close()
		this.pages = []
		this.map.clear()
	}

	async get(id: string): Promise<Idable | undefined> {
		if (!this.index) {
			throw new Error('DB not open')
		}
		const entry = this.map.get(id)
		if (!entry) return undefined

		const page = this.pages[entry.f]
		if (!page) return
		const value = (await readValue(page, entry.o, entry.s)) as Idable
		if (!value.id) {
			throw new Error('ID missing in value')
		}
		if (value.id !== id) {
			throw new Error('ID mismatch')
		}
		return value
	}

	async set<T extends Idable>(id: string, value: T) {
		if (!id) {
			throw new Error('id is required')
		}

		if (!this.index) {
			throw new Error('DB not open')
		}

		value.id = id
		const json = `${JSON.stringify(value)}\n`

		// find the page with the least size to store the data
		let page: Page | undefined = undefined
		for (let i = 0; i < this.pages.length; i++) {
			const p = this.pages[i]
			if (p && p.size < MAX_PAGE_SIZE && i !== this.lastUsedPage) {
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
		this.map.set(id, entry)
	}

	async delete(id: string) {
		if (!id) {
			throw new Error('id is required')
		}

		if (!this.index) {
			throw new Error('DB not open')
		}

		const entry = this.map.get(id)
		if (!entry) return

		// mark the entry as deleted
		await writeValue(this.index, `-${id}\n`)
		this.map.delete(id)
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
async function readJsonNlFile(filePath: string, map: Map<string, MapEntry>) {
	const stream = createReadStream(filePath)
	const rl = createInterface({
		input: stream,
		crlfDelay: Number.POSITIVE_INFINITY,
	})

	for await (const line of rl) {
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
}

// Page related functions
// open a page record or create a new one
async function openPage(pagePath: string): Promise<Page> {
	const handle = await open(pagePath, 'a+')
	const stats = await handle.stat()
	return {
		fileName: pagePath,
		locked: false,
		handle,
		size: stats.size,
		mutex: new Mutex(),
	}
}

async function readValue(page: Page, offset: number, length: number): Promise<unknown> {
	const value = await page.handle.read(Buffer.alloc(length), 0, length, offset)
	return JSON.parse(value.buffer.toString())
}

// write a value to the end of the page
async function writeValue(page: Page, value: string): Promise<number> {
	// const release = await page.mutex.lock()
	// It is unsafe to use filehandle.write() multiple times on the same file
	// without waiting for the promise to be fulfilled (or rejected).
	try {
		const buffer = Buffer.from(value)
		const written = await page.handle.write(buffer, 0, buffer.length, -1)
		page.size += written.bytesWritten
		// await page.handle.datasync()
		return written.bytesWritten
	} finally {
		// release()
	}
}
