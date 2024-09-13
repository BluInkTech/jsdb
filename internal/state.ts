import type { Id, Idable, JsDbOptions } from '../index.js'
import { BLOCK_EXTENSION, type BlockStats, type Storage } from './storage.js'
import { createNodeStorage } from './storage_node.js'
import { generateId } from './utils.js'

export type BlockInfo = BlockStats & { locked: boolean; staleBytes: number }

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
 * Database internal state to be shared around.
 * @internal
 */
export type DbState = {
	idMap: Map<Id, MapEntry>
	ridMap: Map<number, MapEntry>
	blocks: BlockInfo[]
	storage: Storage
	lastUsedBid: number
	seqNo: number
	ridNo: number
	timers: NodeJS.Timeout[]
	opts: JsDbOptions
	opened: boolean
}

enum Operation {
	ItemSet = 1,
	ItemDelete = 2,
}

/**
 * Create a JsDbOptions object with the given options. If the options are not
 * valid, an error will be thrown.
 * @param options options to create a JsDbOptions object
 * @returns a JsDbOptions object
 */
export function createOptions(options: Partial<JsDbOptions>): JsDbOptions {
	const opts: JsDbOptions = {
		dirPath: '',
		maxBlockSize: 1024 * 1024 * 8, // 8 MB
		dataSyncDelay: 1000,
		staleDataThreshold: 0.1,
		compactDelay: 1000 * 60 * 60 * 24, // 24 hours
		cachedFields: [],
	}

	if (!options.dirPath) {
		throw new Error('dirPath is required')
	}
	opts.dirPath = options.dirPath

	if (options.maxBlockSize) {
		if (options.maxBlockSize < 1024 || options.maxBlockSize % 1024 !== 0) {
			throw new Error('maxBlockSize must be at least 1024 KB')
		}
		opts.maxBlockSize = options.maxBlockSize
	}

	if (
		options.dataSyncDelay !== undefined &&
		Number.isInteger(options.dataSyncDelay)
	) {
		opts.dataSyncDelay = options.dataSyncDelay
	}

	if (options.staleDataThreshold) {
		if (options.staleDataThreshold < 0 || options.staleDataThreshold > 1) {
			throw new Error('staleDataThreshold must be between 0 and 1')
		}
		opts.staleDataThreshold = options.staleDataThreshold
	}

	if (options.compactDelay) {
		opts.compactDelay = options.compactDelay
	}

	if (
		options.cachedFields &&
		Array.isArray(options.cachedFields) &&
		options.cachedFields.length > 0
	) {
		opts.cachedFields = options.cachedFields
	}
	return opts
}

export async function createDbState(opts: JsDbOptions): Promise<DbState> {
	const idMap = new Map()
	const ridMap = new Map()

	const storage = await createNodeStorage(opts.dirPath, opts.dataSyncDelay)
	const blocks: BlockInfo[] = (await storage.getBlocksStats()).map(
		(block: BlockStats) => ({
			...block,
			staleBytes: 0,
			locked: false,
		}),
	)

	const blockMaps = await Promise.all(
		blocks.map((b) => loadBlock(b.bid, storage, opts.cachedFields)),
	)

	mergeBlockMaps(idMap, ...blockMaps)
	for (const [, entry] of idMap) {
		ridMap.set(entry._rid, entry)
	}

	// set the sequence number
	const seqNo = sequenceNo(ridMap, Math.max, '_seq')
	const ridNo = sequenceNo(ridMap, Math.max, '_rid')

	// if there are no blocks, create a new one
	if (blocks.length === 0) {
		const bid = `${generateId()}${BLOCK_EXTENSION}`
		await storage.createBlock(bid)
		blocks.push({
			bid: bid,
			size: 0,
			staleBytes: 0,
			locked: false,
		})
	}

	const state: DbState = {
		idMap,
		ridMap: ridMap,
		blocks,
		storage,
		lastUsedBid: 0,
		seqNo,
		ridNo: ridNo,
		timers: [],
		opts,
		opened: true,
	}

	calculateStaleBytes(state)
	// Object.freeze(state)
	return state
}

// check if a valid ID is provided
function isValidId(id: Id): asserts id is string {
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
export async function closeDb(state: DbState): Promise<void> {
	dbIsOpen(state)

	// clear all the timers to avoid memory leaks
	for (const timer of state.timers) {
		clearInterval(timer)
	}

	try {
		await state.storage.close()
	} finally {
	}
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
export function hasItem(state: DbState, id: string): boolean {
	isValidId(id)
	dbIsOpen(state)

	const entry = state.idMap.get(id)
	if (!entry) return false
	if (entry._oid === 2) return false
	return true
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
export function getItem(state: DbState, id: string): Idable | undefined {
	isValidId(id)
	dbIsOpen(state)

	const entry = state.idMap.get(id)
	if (!entry) return undefined
	if (entry._oid === 2) return undefined
	const value = JSON.parse(entry.record)
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
export function setItem(state: DbState, id: Id, value: Idable): Idable {
	isValidId(id)
	dbIsOpen(state)

	const existing = state.idMap.get(id)
	value.id = id
	value._seq = ++state.seqNo
	if (!existing) {
		value._rid = ++state.ridNo
	} else {
		// In case user changed it
		value._rid = existing._rid
	}

	value._oid = Operation.ItemSet

	const json = JSON.stringify(value)
	const bid = getFreeBlock(state)
	const entry = {
		id: value.id,
		bid: bid,
		_oid: value._oid,
		_rid: value._rid,
		_seq: value._seq,
		record: json,
	}

	state.idMap.set(value.id, entry)
	state.ridMap.set(value._rid, entry)

	// update stale entries
	updateStaleBytes(state, entry, existing)

	// update the indexes and cache

	state.storage.appendToBlock(bid, json).catch((error) => {
		state.opts.onError?.(
			new Error(
				`Unable to complete the last set operation with id: ${value.id}. ` +
					`The situation is unrecoverable. Underlying error: ${error}.`,
			),
		)
	})

	return value
}

/**
 * Delete a record from the database.
 * @param id Id of the record
 * @throws Error if the database is not open
 * @throws Error if a valid ID is not provided
 */
export function deleteItem(state: DbState, id: string) {
	isValidId(id)
	dbIsOpen(state)

	const existing = state.idMap.get(id)
	if (!existing) return

	state.idMap.delete(id)
	state.ridMap.delete(existing._rid)

	const bid = getFreeBlock(state)
	const value = {
		id: existing.id,
		_oid: Operation.ItemDelete,
		_rid: existing._rid,
		_seq: ++state.seqNo,
	} as MapEntry
	const json = JSON.stringify(value)

	// setting the below to correctly update the stale entries, but we don't want
	// to write these
	value.bid = bid
	value.record = json
	updateStaleBytes(state, value, existing)

	// update the indexes and cache

	state.storage.appendToBlock(bid, json).catch((error) => {
		state.opts.onError?.(
			new Error(
				`Unable to complete the last delete operation with id: ${value.id}. ` +
					`The situation is unrecoverable. Underlying error: ${error}.`,
			),
		)
	})
}

/**
 * Get a free block for writing and if no block is found, create a new one.
 * @param state current state
 * @returns blockId that can be used for writing
 */
export function getFreeBlock(state: DbState): string {
	let bid = ''
	const length = state.blocks.length
	const startIdx =
		state.lastUsedBid >= 0 && state.lastUsedBid < length ? state.lastUsedBid : 0

	// Check if the current block is available
	if (
		state.blocks[startIdx] &&
		!state.blocks[startIdx].locked &&
		state.blocks[startIdx].size < state.opts.maxBlockSize
	) {
		state.lastUsedBid = startIdx
		bid = state.blocks[startIdx].bid
	} else {
		// If the current block is not available, find the next available block
		for (let i = startIdx + 1; i < length; i++) {
			const p = state.blocks[i]
			if (p && !p.locked && p.size < state.opts.maxBlockSize) {
				state.lastUsedBid = i // Update lastIdx to the index of the selected block
				bid = p.bid
				break
			}
		}
	}

	// if no block is found, create a new one
	if (!bid) {
		bid = `${generateId()}${BLOCK_EXTENSION}`
		state.blocks.push({
			bid: bid,
			size: 0,
			staleBytes: 0,
			locked: false,
		})
		state.lastUsedBid = length
	}

	return bid
}

/**
 * Calculate the stale bytes in a page by summing the sizes of all values in a
 * page minus the size of the page
 * @param pages The pages to calculate stale bytes for
 * @param map The map of entries
 */
export function calculateStaleBytes(state: DbState) {
	const totalData: Record<string, number> = {}
	for (const [_, entry] of state.ridMap) {
		// TODO: record length is not the actual size of the record, we need to
		// calculate the size of the utf8 encoded record
		totalData[entry.bid] = (totalData[entry.bid] || 0) + entry.record.length
	}

	for (let i = 0; i < state.blocks.length; i++) {
		const block = state.blocks[i]
		if (!block) continue
		block.staleBytes = block.size - (totalData[block.bid] || 0)
	}
}

/**
 * Update the stale bytes for a page based on the map entry.
 * When a new entry is added, the existing entry if any will become stale.
 *
 * TODO: record length is not the actual size of the record, we need to
 * calculate the size of the utf8 encoded record
 *
 * @param id The id of the entry
 * @param pages The pages to update
 * @param map The map of entries
 */
export function updateStaleBytes(
	state: DbState,
	newEntry: MapEntry,
	oldEntry?: MapEntry,
) {
	if (oldEntry) {
		const oldBlock = state.blocks.find((b) => b.bid === oldEntry.bid)
		if (oldBlock) {
			oldBlock.staleBytes += oldEntry.record.length
		} else {
			throw new Error(
				'INTERNAL:Page not found. We have a page entry but the page is missing.',
			)
		}
	}

	const newPage = state.blocks.find((b) => b.bid === newEntry.bid)
	if (newPage) {
		newPage.size += newEntry.record.length
	}
}

/**
 * Read a JSON new line file and return a map of entries.
 * @param filePath full path to the file
 * @param blockType type of the block (delete or append)
 * @param fileNumber number of the file
 * @cacheFields fields to cache in memory
 * @returns a map of entries
 */
export async function loadBlock(
	bid: string,
	storage: Storage,
	cacheFields?: string[],
): Promise<Map<Id, MapEntry>> {
	const map = new Map<Id, MapEntry>()
	for await (const [record, lineNo] of storage.readBlock(bid)) {
		try {
			const json = JSON.parse(record) as Idable
			missingAndTypeCheck(json, 'id', 'string')
			missingAndTypeCheck(json, '_oid', 'number')
			missingAndTypeCheck(json, '_rid', 'number')
			missingAndTypeCheck(json, '_seq', 'number')

			const entry: MapEntry = {
				_oid: json._oid,
				_rid: json._rid,
				_seq: json._seq,
				id: json.id,
				bid,
				record,
			}

			if (cacheFields) {
				entry.cache = extractCacheFields(json, cacheFields)
			}

			map.set(json.id, entry)
		} catch (error) {
			throw new Error(
				`Invalid JSON entry in ${bid} at lineNo:${lineNo}: ${(error as Error).message}`,
			)
		}
	}

	return map
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
 * Merge multiple block maps into a single map. The entries with the highest sequence number
 * will be used in case of conflicts.
 * @param target The target map to merge into
 * @param maps The maps to merge
 */
export function mergeBlockMaps(
	target: Map<Id, MapEntry>,
	...maps: Map<Id, MapEntry>[]
) {
	if (maps.length === 0) {
		return
	}

	for (const map of maps) {
		for (const [key, value] of map) {
			const existing = target.get(key)
			// use greater than as compaction might result in same sequence number
			// but offset might be different.
			if (existing && existing._seq > value._seq) {
				continue
			}
			target.set(key, value)
		}
	}
}

/**
 * Get the maximum/minimum sequence number in a block group.
 * @param map Source map
 * @param comparator Math.max or Math.min
 * @param field field to get the sequence number for
 * @param bid block Id to get the sequence number for a specific block
 * @returns the maximum sequence number in source by a specific block
 */
export function sequenceNo(
	src: Map<Id, MapEntry>,
	comparator: Math['max'] | Math['min'],
	field: '_seq' | '_rid',
	bid?: string,
): number {
	let value = comparator === Math.max ? 0 : Number.MAX_SAFE_INTEGER
	for (const [, entry] of src) {
		if (bid && entry.bid !== bid) {
			continue
		}
		value = comparator(value, entry[field])
	}
	return value
}
