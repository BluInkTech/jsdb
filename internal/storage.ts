/**
 * @module internal/storage
 * @internal
 * Deal with the storage of data in the database. The idea is to abstract out the
 * storage mechanism so that it can be easily replaced with a different one. For
 * example, the current implementation uses the file system, but it could be
 * replaced with IndexedDB or a cloud storage service.
 *
 * It should not know or care about the data format, only how to store and retrieve
 */

export const BLOCK_EXTENSION = '.block'
export const BLOCK_SIZE = 1024 * 1024 * 8 // 8 MB

export interface BlockStats {
	bid: string
	size: number
}

export interface Storage {
	appendToBlock: (name: string, entry: string) => Promise<void>
	close: () => Promise<void>
	closeBlock: (bid: string) => Promise<void>
	createBlock: (bid: string) => void
	deleteBlock: (bid: string) => Promise<void>
	getBlocksStats: () => Promise<BlockStats[]>
	getBlockStats: (bid: string) => Promise<BlockStats>
	readBlock: (bid: string) => AsyncIterableIterator<[string, number]>
	renameBlock: (oldBid: string, newBid: string) => Promise<void>
}
