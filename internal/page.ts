import { type FileHandle, open } from 'node:fs/promises'
import path from 'node:path'
import type { Idable } from '../index.js'
import type { MapEntry, Page } from './types.js'

/**
 * Open a page file for reading and writing. If the file does not exist, it will be created.
 * @param pagePath full path to the file
 * @returns a page object
 */
export async function openOrCreatePageFile(pagePath: string): Promise<Page> {
	const handle = await open(pagePath, 'a+')
	const stats = await handle.stat()
	return {
		fileName: pagePath,
		pageNo: Number.parseInt(path.basename(pagePath, '.page')),
		locked: false,
		handle,
		size: stats.size,
		staleBytes: 0,
		close: async () => {
			await handle.close()
		},
	}
}

/**
 * Read a JSON new line file and return a map of entries.
 * @param filePath full path to the file
 * @cacheFields fields to cache in memory
 * @returns a map of entries
 */
export async function readPageFile<T>(
	filePath: string,
	pageType: 'delete' | 'append',
	cacheFields?: string[],
): Promise<Map<string, T>> {
	const map = new Map<string, T>()
	const pathWithoutExtension = path.basename(filePath, '.page')
	if (!pathWithoutExtension) {
		throw new Error('Invalid file name', {
			cause: "A valid page file must end with a '.page' extension",
		})
	}

	const fileNumber = Number.parseInt(pathWithoutExtension)
	if (Number.isNaN(fileNumber)) {
		throw new Error('Invalid file name', {
			cause: "A valid page file must be a number ending with a '.page' extension",
		})
	}

	const handle = await open(filePath, 'a+')
	let offset = 0
	let lineNo = 0
	try {
		for await (const line of handle.readLines()) {
			if (lineNo !== 0) {
				offset++ // for the new line character
			}
			const start = offset
			offset += Buffer.byteLength(line)
			lineNo++
			if (line === '') continue

			try {
				const json = JSON.parse(line) as Idable
				if (!json.id || !json._seq) {
					// Not sure if this should throw
					continue
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
					continue
				}

				const entry: MapEntry = {
					fileNumber: 0,
					offset: start,
					size: line.length,
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
		}
	} finally {
		// Close the file even if an error occurred
		await handle.close()
	}

	return map
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
