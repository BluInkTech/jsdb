import fsp from 'node:fs/promises'
import type { Id, Idable } from '../index.js'
import { mergePageMaps } from './pagegroup.js'
import { generateId, throttle } from './utils.js'
/**
 * A page represents a file in the database. Each page is
 * a self contained database.
 * @internal
 */
export type Page = {
	fileName: string
	pageId: string
	locked: boolean
	closed: boolean
	handle: fsp.FileHandle
	ws: NodeJS.WritableStream
	size: number
	staleBytes: number
	close: () => Promise<void>
	flush: () => Promise<void>
}

/**
 * Contains the information of a map entry.
 * @internal
 */
export type MapEntry = {
	_oid: number // operation identifier
	_rid: number // record identifier
	_seq: number // sequence number
	id: Id // unique identifier
	bid: string // file identifier
	record: string // the record saved as a string
	cache?: unknown // inline data used for fast access
}

/**
 * Open a page file for reading and writing. If the file does not exist, it will be created.
 * @param pagePath full path to the file
 * @returns a page object
 */
export async function openOrCreatePageFile(pagePath: string): Promise<Page> {
	const handle = await fsp.open(pagePath, 'a+')
	const ws = handle.createWriteStream({
		highWaterMark: 1024 * 4,
		encoding: 'utf8',
		autoClose: false,
	})
	const stats = await fsp.stat(pagePath)

	const page: Page = {
		fileName: pagePath,
		pageId: path.basename(pagePath),
		locked: false,
		closed: false,
		size: stats.size,
		staleBytes: 0,
		handle,
		ws,
		close: async () => {
			try {
				// there is not much that can be done here
				page.ws.end()
				await handle.close()
			} catch (error) {
				// ignore the error if the file is already closed
				if ((error as NodeJS.ErrnoException).code !== 'EBADF') {
					throw error
				}
			} finally {
				page.closed = true
			}
		},
		flush: async () => {
			try {
				await handle.datasync()
			} catch (error) {
				// ignore the error if the file is already closed
				if ((error as NodeJS.ErrnoException).code !== 'EBADF') {
					throw error
				}
			}
		},
	}
	return page
}

/**
 * Checks if a specific field exists in a JSON object and if its type matches the
 * specified type.
 * @param json - The JSON object to check.
 * @param field - The field to check in the JSON object.
 * @param type - The expected type of the field. Can be 'string', 'number', or 'boolean'.
 * @throws {Error} If the field is missing or if its type does not match the specified type.
 */
export function missingAndTypeCheck(
	// biome-ignore lint/suspicious/noExplicitAny: should work with any type
	json: any,
	field: string,
	type: 'string' | 'number' | 'boolean',
) {
	if (json[field] === undefined) {
		throw new Error(`${field} is missing`)
	}

	// biome-ignore lint/suspicious/useValidTypeof: We want to check it dynamically
	if (typeof json[field] !== type) {
		throw new Error(`${field} must be a ${type}`)
	}
}

/**
 * Extracts specified cache fields from a JSON object and returns them as a
 * separate cache object.
 * @param json - The JSON object from which to extract cache fields.
 * @param cacheFields - An array of strings representing the cache fields to extract.
 * @returns A cache object containing the extracted cache fields.
 */
export function extractCacheFields(
	json: Record<string, unknown>,
	cacheFields: string[],
) {
	const cache: Record<string, unknown> = {}
	for (const field of cacheFields) {
		if (json[field] !== undefined) {
			cache[field] = json[field]
		}
	}
	return cache
}
/**
 * Read a JSON new line file and return a map of entries.
 * @param filePath full path to the file
 * @param pageType type of the page (delete or append)
 * @param fileNumber number of the file
 * @cacheFields fields to cache in memory
 * @returns a map of entries
 */
export async function readPageFile(
	filePath: string,
	cacheFields?: string[],
): Promise<Map<Id, MapEntry>> {
	const pageId = path.basename(filePath)

	const map = new Map<Id, MapEntry>()
	await readLines(filePath, (buffer, _offset, _size, lineNo) => {
		try {
			const src = buffer.toString('utf8')
			const json = JSON.parse(src) as Idable
			missingAndTypeCheck(json, 'id', 'string')
			missingAndTypeCheck(json, '_oid', 'number')
			missingAndTypeCheck(json, '_rid', 'number')
			missingAndTypeCheck(json, '_seq', 'number')

			const entry: MapEntry = {
				_oid: json._oid,
				_rid: json._rid,
				_seq: json._seq,
				id: json.id,
				bid: pageId,
				record: src,
			}

			if (cacheFields) {
				entry.cache = extractCacheFields(json, cacheFields)
			}

			map.set(json.id, entry)
		} catch (error) {
			throw new Error(
				`Invalid JSON entry in ${filePath} at lineNo:${lineNo}: ${(error as Error).message}`,
			)
		}
	})
	return map
}

/**
 * Read a file line by line and process each line with a function.
 * @param filePath The path to the file to read
 * @param processLine function to process each line
 * @param bufferSize size of the buffer to read (default 1024)
 * @param breakChar character to break the line (default '\n')
 */
export async function readLines(
	filePath: string,
	processLine: (
		buffer: Buffer,
		offset: number,
		size: number,
		lineNo: number,
	) => void,
	bufferSize = 1024,
	breakChar = '\n',
): Promise<void> {
	const stream = await fsp.open(filePath, 'r')
	const buffer = Buffer.alloc(bufferSize)

	const breakCharCode = breakChar.charCodeAt(0)
	let bytesConsumed = 0
	let lineNo = 0
	let remaining = Buffer.alloc(0)

	try {
		let b = await stream.read(buffer, 0, bufferSize)
		// we assume that the file will always end with a break character
		// so the last record is ignored if it doesn't end with a break character
		while (b.bytesRead !== 0) {
			const concatBuffer = Buffer.concat([
				remaining,
				b.buffer.subarray(0, b.bytesRead),
			])
			let start = 0
			let posBreakChar = concatBuffer.indexOf(breakCharCode, start)
			while (posBreakChar !== -1) {
				lineNo++
				const line = concatBuffer.subarray(start, posBreakChar)
				if (line.length === 0) {
					throw new Error(`Empty line in file:${filePath}`, {
						cause: `There is an empty line at line number${lineNo}. Fix the file manually.`,
					})
				}
				// calculate the size of the line
				processLine(line, bytesConsumed, Buffer.byteLength(line), lineNo)
				start = posBreakChar + 1 // skip the break character

				// Update bytesConsumed with the actual byte length of the processed line
				bytesConsumed += Buffer.byteLength(line) + 1 // +1 for the break character

				// find the next break point in the current buffer
				posBreakChar = concatBuffer.indexOf(breakCharCode, start)
			}

			// if there is a remaining unread buffer, append it to the remaining buffer
			// so that it is picked in the next iteration
			if (start < concatBuffer.length) {
				remaining = concatBuffer.subarray(start)
			} else {
				remaining = Buffer.alloc(0)
			}

			b = await stream.read(buffer, 0, bufferSize)
		}
	} finally {
		await stream.close()
	}
}

// write a value to the end of the page
export async function writeValue(
	page: Page,
	value: Buffer,
	sync: number,
): Promise<number> {
	if (sync === 0) {
		fs.writeSync(page.handle.fd, value, 0, value.byteLength)
		page.size += value.byteLength
		return value.byteLength
	}

	const written = page.ws.write(value, (err) => {
		if (err) {
			throw err
		}
	})

	if (!written) {
		await new Promise<void>((resolve) => {
			page.ws.once('drain', () => {
				resolve()
			})
		})
	}

	// Throttle the datasync operation to avoid frequent disk writes
	throttle(() => {
		page.flush()
	}, sync)()

	// update the size of the page
	page.size += value.byteLength
	return value.byteLength
}

/**
 * Compact a page by removing stale entries and rewriting the page.
 * The compaction is done in following steps:
 * - Mark the page as locked
 * - Create a new page file with a random name and write all the valid entries to it.
 * - Valid entries are those which have page number as the current page number.
 * - Entries are written by reading the value from the old page and writing it to the new page.
 * - Create a new map of entries with the new offsets and page number.
 * - Merge the entries (offsets are changed now) with the map.
 * - In case the record is updated while compaction, the new record will have a higher sequence number.
 * - Once the new page is read, then rename the new page file to the old page file.
 * - Mark the page as unlocked.
 * @param map The map of entries in the database
 * @param pages The list of pages in the database
 * @param page The page to compact
 * @param filterSeqNo The sequence number below which entries are filtered out
 */
export async function compactPage(
	map: Map<string, MapEntry>,
	pages: Page[],
	page: Page,
	filterSeqNo = 0,
): Promise<void> {
	if (page.locked) {
		throw new Error('Page is already locked')
	}

	page.locked = true
	const rootDir = path.dirname(page.fileName)
	const newPageId = generateId()

	const newPagePath = path.join(rootDir, `${newPageId}.tmp`)
	const newPageHandle = await fsp.open(newPagePath, 'a+')

	const newMap = new Map<string, MapEntry>()
	let size = 0
	try {
		const newPageName = `${newPageId}.page`
		for (const [id, entry] of map.entries()) {
			if (entry._seq < filterSeqNo) {
				continue
			}
			if (entry.bid === page.pageId) {
				const oldValue = entry.record
				const written = await newPageHandle.write(oldValue)

				// we can increment the sequence number here but we are not doing it
				// as the sequence number can also be used for optimistic updates. If the sequence
				// number is changed then the user caching the value will get an error even
				// though the data has not changed.
				// Every page compaction should not invalidate the user cache.
				newMap.set(id, {
					...entry,
					bid: newPageName,
				})
				size += written.bytesWritten
			}
		}
	} finally {
		await newPageHandle.close()
	}

	// rename the new page and open it again.
	const renamedPageFile = newPagePath.replace('.tmp', '.page')
	await fsp.rename(newPagePath, renamedPageFile)
	const newPage = await openOrCreatePageFile(renamedPageFile)

	// we need to merge the new map with the main map
	mergePageMaps(map, newMap)

	// remove the old page entry from the pages list and add a new page entry
	const index = pages.findIndex((p) => p.pageId === page.pageId)
	pages.splice(index, 1)
	pages.push(newPage)

	// purge any stale entries from map related to the old page
	for (const [id, entry] of map.entries()) {
		if (entry.bid === page.pageId) {
			map.delete(id)
		}
	}

	// rename the old page file. If this fails then we will have a database which
	// will have two entries with same sequence number. This is not a problem as
	// the next load will pick one of the two files. The other file will have a lot
	// of stale entries and will be cleaned up eventually.
	await page.close()
	await fsp.rename(page.fileName, `${page.fileName}.old`)
}
