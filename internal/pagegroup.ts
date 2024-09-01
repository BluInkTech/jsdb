/**
 * Helpers which work with pages or page groups.
 */
import path from 'node:path'
import type { Idable, JsDbOptions } from '../index.js'
import { getFilesWithExtension } from './io.js'
import { openOrCreatePageFile, readPageFile, writeValue } from './page.js'
import type { MapEntry, Page } from './page.js'
import { generateId } from './utils.js'

/**
 * A group of pages that share the same extension.
 * @internal
 */
export type PageGroup = {
	map: Map<string, MapEntry>
	extension: string
	pages: Page[]
	dirPath: string
	lastIdx: number
	dataSyncDelay: number
	maxPageSize: number
	maxStaleBytes: number
}

/**
 * Create a new page group from a directory of files with a specific extension.
 * @param dirPath path to the root directory
 * @param extension file extension to search for
 * @param opts JsDbOptions object
 * @returns a new page group
 */
export async function createPageGroup(
	dirPath: string,
	extension: string,
	opts: JsDbOptions,
): Promise<PageGroup> {
	const map = new Map<string, MapEntry>()
	const files = await getFilesWithExtension(dirPath, extension)
	const pageMaps = await Promise.all(
		files.map((file) => readPageFile(file, opts.cachedFields)),
	)

	mergePageMaps(map, ...pageMaps)

	const pages = await Promise.all(
		files.map((file) => openOrCreatePageFile(file)),
	)

	return {
		map,
		extension,
		pages,
		lastIdx: 0,
		dirPath: opts.dirPath,
		maxPageSize: opts.maxPageSize,
		maxStaleBytes: opts.maxPageSize * opts.staleDataThreshold,
		dataSyncDelay: opts.dataSyncDelay,
	}
}

/**
 * Get the maximum sequence number in a page group.
 * @param pg PageGroup
 * @param pageId pageId to get the sequence number for a specific page
 * @returns the maximum sequence number in the page group or page
 */
export function maxSequenceNo(pg: PageGroup, pageId?: string): number {
	let max = 0
	for (const [, entry] of pg.map) {
		if (pageId && entry.pageId !== pageId) {
			continue
		}
		if (entry._seq > max) {
			max = entry._seq
		}
	}
	return max
}

/**
 * Get the minimum sequence number in a page group.
 * @param pg PageGroup
 * @param pageId pageId to get the sequence number for a specific page
 * @returns the minimum sequence number in the page group or page
 */
export function minSequenceNo(pg: PageGroup, pageId?: string) {
	let min = Number.MAX_SAFE_INTEGER
	for (const [, entry] of pg.map) {
		if (pageId && entry.pageId !== pageId) {
			continue
		}
		if (entry._seq < min) {
			min = entry._seq
		}
	}
	return min
}

/**
 * Get a free page for writing and if no page is found, create a new one.
 * @param pages current pages
 * @param maxPageSize maximum allowed page size
 * @param lastUsedPageIdx page number of the last used page
 * @param basePath base path for the page files
 * @param createPage function to create a new page
 * @returns a page that can be used for writing
 */
export async function getfreePage(
	pg: PageGroup,
	createPage: (pagePath: string) => Promise<Page> = openOrCreatePageFile,
): Promise<Page> {
	let page: Page | undefined = undefined

	const length = pg.pages.length
	const startIdx = pg.lastIdx >= 0 && pg.lastIdx < length ? pg.lastIdx : 0

	// Check if the current page is available
	if (
		pg.pages[startIdx] &&
		!pg.pages[startIdx].locked &&
		pg.pages[startIdx].size < pg.maxPageSize
	) {
		page = pg.pages[startIdx]
	} else {
		// If the current page is not available, find the next available page
		for (let i = startIdx + 1; i < length; i++) {
			const p = pg.pages[i]
			if (p && !p.locked && p.size < pg.maxPageSize) {
				page = p
				pg.lastIdx = i // Update lastIdx to the index of the selected page
				break
			}
		}
	}

	// if no page is found, create a new one
	if (!page) {
		page = await createPage(
			path.join(pg.dirPath, `${generateId()}${pg.extension}`),
		)
		pg.pages.push(page)
	}
	return page
}

export async function appendToFreePage(
	pg: PageGroup,
	seqNo: number,
	value: Idable,
): Promise<MapEntry> {
	const page = await getfreePage(pg)

	// write the value to the page
	const offset = page.size
	value._seq = seqNo
	const jsonStr = `${JSON.stringify(value)}\n`
	const bytesWritten = await writeValue(
		page,
		Buffer.from(jsonStr),
		pg.dataSyncDelay,
	)

	const entry: MapEntry = {
		pageId: page.pageId,
		offset: offset,
		size: bytesWritten,
		_seq: value._seq,
	}

	return entry
}

/**
 * Merge multiple page maps into a single map. The entries with the highest sequence number
 * will be used in case of conflicts.
 * @param target The target map to merge into
 * @param maps The maps to merge
 */
export function mergePageMaps(
	target: Map<string, MapEntry>,
	...maps: Map<string, MapEntry>[]
) {
	if (maps.length === 0) {
		return
	}

	for (const map of maps) {
		for (const [key, value] of map) {
			const existing = target.get(key)
			// use greater than equal to as compaction might result in same sequence number
			// but offset might be different.
			if (existing && existing._seq >= value._seq) {
				continue
			}
			target.set(key, value)
		}
	}
}

/**
 * Remove all entries that have a delete operation.
 * @param target The target map to remove entries from
 */
export function removeDeletedEntires(
	target: Map<string, MapEntry>,
	deleteMap: Map<string, MapEntry>,
) {
	for (const [key, value] of deleteMap) {
		const existing = target.get(key)
		if (existing) {
			// delete the entry if the existing sequence number is
			// less than the delete sequence number. The sequence numbers
			// can never be equal as these are two different operations.
			if (existing._seq < value._seq) {
				target.delete(key)
			}
		}
	}
}

/**
 * Calculate the stale bytes in a page by summing the sizes of all values in a
 * page minus the size of the page
 * @param pages The pages to calculate stale bytes for
 * @param map The map of entries
 */
export function calculateStaleBytes(pages: Page[], map: Map<string, MapEntry>) {
	const totalData: Record<string, number> = {}
	for (const [_, entry] of map) {
		totalData[entry.pageId] = (totalData[entry.pageId] || 0) + entry.size
	}

	for (let i = 0; i < pages.length; i++) {
		const page = pages[i]
		if (!page) continue
		page.staleBytes = page.size - (totalData[page.pageId] || 0)
	}
}

/**
 * Update the stale bytes for a page based on the map entry.
 * When a new entry is added, the existing entry if any will become stale.
 * @param id The id of the entry
 * @param pages The pages to update
 * @param map The map of entries
 */
export function updateStaleBytes(
	id: string,
	pages: Page[],
	map: Map<string, MapEntry>,
) {
	const existingEntry = map.get(id)
	if (existingEntry) {
		const existingPage = pages.find((p) => p.pageId === existingEntry.pageId)
		if (existingPage) {
			existingPage.staleBytes += existingEntry.size
		} else {
			throw new Error(
				'INTERNAL:Page not found. We have a page entry but the page is missing.',
			)
		}
	}
}
