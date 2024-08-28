import path from 'node:path'
import { openOrCreatePageFile } from './page.js'
import type { MapEntry, Page } from './types.js'

/**
 * Find a page that can be used for writing. The page must not be locked, not be the
 * last used page and not be full.
 * @param pages current pages
 * @param maxPageSize maximum allowed page size
 * @param lastUsedPageIdx page number of the last used page
 * @returns a page that can be used for writing or undefined if no page is available
 */
function findFreePage(
	pages: Page[],
	maxPageSize: number,
	lastUsedPageIdx: number,
): Page | undefined {
	// NOTE: for loop with cached length is faster than other methods like for of etc. The below code is
	// in the hot path.
	const length = pages.length
	const startIdx =
		lastUsedPageIdx >= 0 && lastUsedPageIdx < length ? lastUsedPageIdx + 1 : 0
	for (let i = startIdx; i < length; i++) {
		// @ts-ignore - pages[i] can't be undefined as we are iterating over the array after checking its length
		if (
			!pages[i].locked &&
			pages[i].pageId !== lastUsedPageIdx &&
			pages[i].size < maxPageSize
		) {
			return pages[i]
		}
	}
	return undefined
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
	pages: Page[],
	maxPageSize: number,
	lastUsedPageIdx: number,
	basePath: string,
	createPage: (pagePath: string) => Promise<Page> = openOrCreatePageFile,
): Promise<Page> {
	let page = findFreePage(pages, maxPageSize, lastUsedPageIdx)

	// if no page is found, create a new one
	if (!page) {
		page = await createPage(path.join(basePath, `${pages.length}.page`))
		pages.push(page)
	}
	return page
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
	deleteMap: Map<string, number>,
) {
	for (const [key, value] of deleteMap) {
		const existing = target.get(key)
		if (existing) {
			// delete the entry if the existing sequence number is
			// less than the delete sequence number. The sequence numbers
			// can never be equal as these are two different operations.
			if (existing._seq < value) {
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
