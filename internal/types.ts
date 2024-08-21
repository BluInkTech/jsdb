import type { FileHandle } from 'node:fs/promises'

/**
 * Contains the information of a map entry.
 * @internal
 */
export type MapEntry = {
	fileNumber: number // file number
	offset: number // offset
	size: number // size
	_seq: number // sequence number
	cache?: unknown // inline data used for fast access
}

/**
 * A page represents a file in the database. Each page is
 * a self contained database.
 * @internal
 */
export type Page = {
	fileName: string
	pageNo: number
	locked: boolean
	handle: FileHandle
	size: number
	staleBytes: number
	close: () => Promise<void>
}
