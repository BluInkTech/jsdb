import { type FileHandle, open, rename } from 'node:fs/promises'
import path from 'node:path'
import type { Idable } from '../index.js'
import { mergePageMaps } from './pages.js'
import type { MapEntry, Page } from './types.js'
import { generateId } from './utils.js'

/**
 * Open a page file for reading and writing. If the file does not exist, it will be created.
 * @param pagePath full path to the file
 * @param extension extension of the file (default 'page')
 * @returns a page object
 */
export async function openOrCreatePageFile(pagePath: string, extension: 'page' | 'tmp' = 'page'): Promise<Page> {
	const handle = await open(pagePath, 'a+')
	const stats = await handle.stat()
	return {
		fileName: pagePath,
		pageId: path.basename(pagePath, `.${extension}`),
		locked: false,
		handle,
		size: stats.size,
		staleBytes: 0,
		close: async () => {
			await handle.close()
		},
	}
}

export async function readPageFile(filePath: string, pageType: 'delete'): Promise<Map<string, number>>
export async function readPageFile(
	filePath: string,
	pageType: 'append',
	cacheFields?: string[],
): Promise<Map<string, MapEntry>>

/**
 * Read a JSON new line file and return a map of entries.
 * @param filePath full path to the file
 * @param pageType type of the page (delete or append)
 * @param fileNumber number of the file
 * @cacheFields fields to cache in memory
 * @returns a map of entries
 */
export async function readPageFile<T>(
	filePath: string,
	pageType: 'delete' | 'append',
	cacheFields?: string[],
): Promise<Map<string, T>> {
	const pageId = path.basename(filePath)

	const map = new Map<string, T>()
	await readLines(filePath, (buffer, offset, size, lineNo) => {
		try {
			const json = JSON.parse(buffer.toString('utf-8')) as Idable
			if (!json.id || json._seq === undefined) {
				throw new Error('id and _seq are required fields')
			}

			// id should be a string
			if (typeof json.id !== 'string') {
				throw new Error('id must be a string')
			}

			// sequence number should be a number
			if (typeof json._seq !== 'number') {
				throw new Error('_seq must be a number')
			}

			// in case of a delete page type we only need the sequence number
			if (pageType === 'delete') {
				map.set(json.id, json._seq as unknown as T)
				return
			}

			const entry: MapEntry = {
				pageId,
				offset,
				size,
				_seq: json._seq,
			}

			if (cacheFields) {
				const cache: Record<string, unknown> = {}
				for (const field of cacheFields) {
					if (json[field]) {
						cache[field] = json[field]
					}
				}
				entry.cache = cache
			}

			map.set(json.id, entry as T)
		} catch (error) {
			throw new Error(`Invalid JSON entry in ${filePath} at lineNo:${lineNo}`, {
				cause: (error as Error).message,
			})
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
	processLine: (buffer: Buffer, offset: number, size: number, lineNo: number) => void,
	bufferSize = 1024,
	breakChar = '\n',
): Promise<void> {
	const stream = await open(filePath, 'r')
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
			const concatBuffer = Buffer.concat([remaining, b.buffer.subarray(0, b.bytesRead)])
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
				// caclulate the size of the line
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

export async function readValue(page: Page, offset: number, length: number): Promise<unknown> {
	const value = await page.handle.read(Buffer.alloc(length), 0, length, offset)
	return JSON.parse(value.buffer.toString())
}

// write a value to the end of the page
export async function writeValue(handle: FileHandle, buffer: Buffer): Promise<number> {
	const written = await handle.write(buffer, 0, buffer.length, -1)
	// TODO: await page.handle.datasync()
	return written.bytesWritten
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
 */
export async function compactPage(map: Map<string, MapEntry>, pages: Page[], page: Page): Promise<void> {
	if (page.locked) {
		throw new Error('Page is already locked')
	}

	page.locked = true
	const rootDir = path.dirname(page.fileName)
	const newPageId = generateId()

	const newPagePath = path.join(rootDir, `${newPageId}.tmp`)
	let newPageHandle = await open(newPagePath, 'a+')
	const newMap = new Map<string, MapEntry>()
	let size = 0
	try {
		for (const [id, entry] of map.entries()) {
			if (entry.pageId === page.pageId) {
				const oldValue = await page.handle.read(Buffer.alloc(entry.size), 0, entry.size, entry.offset)
				const written = await writeValue(newPageHandle, oldValue.buffer)

				// we can increment the sequence number here but we are not doing it
				// as the sequence number can also be used for optimistic updates. If the sequence
				// number is changed then the user caching the value will get an error even
				// though the data has not changed.
				// Every page compaction should not invalidate the user cache.
				newMap.set(id, {
					...entry,
					offset: written,
					pageId: newPageId,
				})
				size += written
			}
		}
	} finally {
		await newPageHandle.close()
	}

	// rename the new page and open it again.
	const renamedPageFile = newPagePath.replace('.tmp', '.page')
	await rename(newPagePath, renamedPageFile)
	newPageHandle = await open(newPagePath, 'a+')

	// all the below operations should be sync
	// we need to merge the new map with the main map
	mergePageMaps(map, newMap)

	// remove the old page entry from the pages list and add a new page entry
	const index = pages.findIndex((p) => p.pageId === page.pageId)
	pages.splice(index, 1)
	pages.push({
		fileName: renamedPageFile,
		pageId: newPageId,
		locked: false,
		handle: newPageHandle,
		size: size,
		staleBytes: 0, // will be recalculated by the timer
		close: async () => {
			await newPageHandle.close()
		},
	})

	// rename the old page file. If this fails then we will have a database which
	// will have two entries with same sequence number. This is not a problem as
	// the next load will pick one of the two files. The other file will have a lot
	// of stale entries and will be cleaned up eventually.
	await page.handle.close()
	await rename(page.fileName, `${page.fileName}.old`)
}
