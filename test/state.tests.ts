import { describe, expect, it } from 'vitest'
import type { JsDbOptions } from '..'
import {
	type BlockInfo,
	type DbState,
	type MapEntry,
	createOptions,
	extractCacheFields,
	getFreeBlock,
	mergeBlockMaps,
	missingAndTypeCheck,
	sequenceNo,
} from '../internal/state'

describe('create option tests', () => {
	it('valid dir path is required', () => {
		expect(() => createOptions({ dirPath: '' })).toThrow('dirPath is required')
	})

	it('max block size must be at least 1024 KB', () => {
		expect(() =>
			createOptions({ dirPath: 'test', maxBlockSize: 1023 }),
		).toThrow('maxBlockSize must be at least 1024 KB')
	})

	it('max block size must be a multiple of 1024', () => {
		expect(() =>
			createOptions({ dirPath: 'test', maxBlockSize: 1025 }),
		).toThrow('maxBlockSize must be at least 1024 KB')
	})

	it('data sync delay is set', () => {
		const opts = createOptions({ dirPath: 'test', dataSyncDelay: 10 })
		expect(opts.dataSyncDelay).toBe(10)
	})

	it('data sync delay of 0 works', () => {
		const opts = createOptions({ dirPath: 'test', dataSyncDelay: 0 })
		expect(opts.dataSyncDelay).toBe(0)
	})

	it('stale data threshold must be between 0 and 1', () => {
		expect(() =>
			createOptions({ dirPath: 'test', staleDataThreshold: -1 }),
		).toThrow('staleDataThreshold must be between 0 and 1')
	})

	it('stale data threshold must be between 0 and 1', () => {
		expect(() =>
			createOptions({ dirPath: 'test', staleDataThreshold: 2 }),
		).toThrow('staleDataThreshold must be between 0 and 1')
	})

	it('compact delay is set', () => {
		const opts = createOptions({ dirPath: 'test', compactDelay: 100 })
		expect(opts.compactDelay).toBe(100)
	})

	it('cached fields are set', () => {
		const opts = createOptions({ dirPath: 'test', cachedFields: ['test'] })
		expect(opts.cachedFields).toEqual(['test'])
	})

	it('default options', () => {
		const opts = createOptions({ dirPath: 'test' })
		expect(opts.maxBlockSize).toBe(1024 * 1024 * 8)
		expect(opts.dataSyncDelay).toBe(1000)
		expect(opts.staleDataThreshold).toBe(0.1)
		expect(opts.compactDelay).toBe(1000 * 60 * 60 * 24)
		expect(opts.cachedFields).toEqual([])
	})

	it('custom options', () => {
		const opts = createOptions({
			dirPath: 'test',
			maxBlockSize: 1024 * 1024 * 16,
			dataSyncDelay: 2000,
			staleDataThreshold: 0.2,
			compactDelay: 1000 * 60 * 60 * 12,
			cachedFields: ['test'],
		})
		expect(opts.maxBlockSize).toBe(1024 * 1024 * 16)
		expect(opts.dataSyncDelay).toBe(2000)
		expect(opts.staleDataThreshold).toBe(0.2)
		expect(opts.compactDelay).toBe(1000 * 60 * 60 * 12)
		expect(opts.cachedFields).toEqual(['test'])
	})
})

describe('sequenceNo', () => {
	const ridMap = new Map<string, MapEntry>()
	ridMap.set('1', { _seq: 10, _rid: 1, bid: 'page1' } as MapEntry)
	ridMap.set('2', { _seq: 20, _rid: 2, bid: 'page2' } as MapEntry)
	ridMap.set('3', { _seq: 15, _rid: 3, bid: 'page1' } as MapEntry)

	it('should return the maximum sequence number for the entire page group', () => {
		const result = sequenceNo(ridMap, Math.max, '_seq')
		expect(result).toBe(20)
	})

	it('should return the minimum sequence number for the entire page group', () => {
		const result = sequenceNo(ridMap, Math.min, '_seq')
		expect(result).toBe(10)
	})

	it('should return the maximum sequence number for a specific page', () => {
		const result = sequenceNo(ridMap, Math.max, '_seq', 'page1')
		expect(result).toBe(15)
	})

	it('should return the minimum sequence number for a specific page', () => {
		const result = sequenceNo(ridMap, Math.min, '_seq', 'page1')
		expect(result).toBe(10)
	})

	it('should return the maximum rid for the entire page group', () => {
		const result = sequenceNo(ridMap, Math.max, '_rid')
		expect(result).toBe(3)
	})

	it('should return the minimum rid for the entire page group', () => {
		const result = sequenceNo(ridMap, Math.min, '_rid')
		expect(result).toBe(1)
	})

	it('should return the maximum rid for a specific page', () => {
		const result = sequenceNo(ridMap, Math.max, '_rid', 'page1')
		expect(result).toBe(3)
	})

	it('should return the minimum rid for a specific page', () => {
		const result = sequenceNo(ridMap, Math.min, '_rid', 'page1')
		expect(result).toBe(1)
	})
})

describe('getFreeBlock', () => {
	it('should return the first free block', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: false, size: 50, staleBytes: 0 },
			{ bid: '2', locked: false, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 0,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('1')
		expect(state.lastUsedBid).toEqual(0)
	})

	it('should return the first unlocked page', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: true, size: 50, staleBytes: 0 },
			{ bid: '2', locked: false, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 0,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('2')
		expect(state.lastUsedBid).toEqual(1)
	})

	it('should return the page with size less than maxPageSize', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: false, size: 150, staleBytes: 0 },
			{ bid: '2', locked: false, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 0,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('2')
		expect(state.lastUsedBid).toEqual(1)
	})

	it('should return the next unlocked page', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: true, size: 50, staleBytes: 0 },
			{ bid: '2', locked: true, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 0,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('3')
		expect(state.lastUsedBid).toEqual(2)
	})

	it('should create a new page if no page is available', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: true, size: 50, staleBytes: 0 },
			{ bid: '2', locked: true, size: 50, staleBytes: 0 },
			{ bid: '3', locked: true, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 0,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toBeDefined()
		// lastIdx will remain unchanged as it will be set by appendToFreePage
		expect(state.lastUsedBid).toEqual(0)
	})

	it('should return the first free page if lastIdx is negative', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: false, size: 50, staleBytes: 0 },
			{ bid: '2', locked: false, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: -1,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('1')
		expect(state.lastUsedBid).toEqual(0)
	})

	it('should return the first free page if lastIdx is out of bounds', async () => {
		const blocks: BlockInfo[] = [
			{ bid: '1', locked: false, size: 50, staleBytes: 0 },
			{ bid: '2', locked: false, size: 50, staleBytes: 0 },
			{ bid: '3', locked: false, size: 50, staleBytes: 0 },
		]
		const state = {
			blocks,
			lastUsedBid: 10,
			opts: { maxBlockSize: 100 } as JsDbOptions,
		} as DbState
		const bid = getFreeBlock(state)
		expect(bid).toEqual('1')
		expect(state.lastUsedBid).toEqual(0)
	})
})

describe('mergePageMaps', () => {
	it('should merge two page maps', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { bid: '1', _seq: 1 } as MapEntry)
		target.set('2', { bid: '2', _seq: 2 } as MapEntry)

		const map1 = new Map<string, MapEntry>()
		map1.set('3', { bid: '3', _seq: 3 } as MapEntry)
		map1.set('4', { bid: '4', _seq: 4 } as MapEntry)

		mergeBlockMaps(target, map1)
		expect(target.size).toBe(4)
		expect(target.get('1')).toMatchObject({
			bid: '1',
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			bid: '2',
			_seq: 2,
		})
		expect(target.get('3')).toMatchObject({
			bid: '3',
			_seq: 3,
		})
		expect(target.get('4')).toMatchObject({
			bid: '4',
			_seq: 4,
		})
	})

	it('should merge multiple page maps', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { bid: '1', _seq: 1 } as MapEntry)
		target.set('2', { bid: '2', _seq: 2 } as MapEntry)

		const map1 = new Map<string, MapEntry>()
		map1.set('3', { bid: '3', _seq: 3 } as MapEntry)
		map1.set('4', { bid: '4', _seq: 4 } as MapEntry)

		const map2 = new Map<string, MapEntry>()
		map2.set('5', { bid: '5', _seq: 5 } as MapEntry)
		map2.set('6', { bid: '6', _seq: 6 } as MapEntry)

		mergeBlockMaps(target, map1, map2)
		expect(target.size).toBe(6)
		expect(target.get('1')).toMatchObject({
			bid: '1',
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			bid: '2',
			_seq: 2,
		})
		expect(target.get('3')).toMatchObject({
			bid: '3',
			_seq: 3,
		})
		expect(target.get('4')).toMatchObject({
			bid: '4',
			_seq: 4,
		})
		expect(target.get('5')).toMatchObject({
			bid: '5',
			_seq: 5,
		})
		expect(target.get('6')).toMatchObject({
			bid: '6',
			_seq: 6,
		})
	})

	it('should not overwrite existing entries', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { bid: '1', _seq: 1 } as MapEntry)
		target.set('2', { bid: '2', _seq: 2 } as MapEntry)

		const map1 = new Map<string, MapEntry>()
		map1.set('2', { bid: '2', _seq: 3 } as MapEntry)
		map1.set('3', { bid: '3', _seq: 4 } as MapEntry)

		mergeBlockMaps(target, map1)
		expect(target.size).toBe(3)
		expect(target.get('1')).toMatchObject({
			bid: '1',
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			bid: '2',
			_seq: 3,
		})
		expect(target.get('3')).toMatchObject({
			bid: '3',
			_seq: 4,
		})
	})

	it('should overwrite existing entries with same sequence number', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { bid: '1', _seq: 1 } as MapEntry)
		target.set('2', { bid: '2', _seq: 2 } as MapEntry)

		const map1 = new Map<string, MapEntry>()
		map1.set('2', { bid: '2', _seq: 2 } as MapEntry)
		map1.set('3', { bid: '3', _seq: 4 } as MapEntry)

		mergeBlockMaps(target, map1)
		expect(target.size).toBe(3)

		expect(target.get('1')).toMatchObject({
			bid: '1',
			_seq: 1,
		})

		expect(target.get('2')).toMatchObject({
			bid: '2',
			_seq: 2,
		})

		expect(target.get('3')).toMatchObject({
			bid: '3',
			_seq: 4,
		})
	})
})

describe('missingAndTypeCheck', () => {
	it('should throw an error if the field is missing', () => {
		const json = { name: 'test' }
		expect(() => missingAndTypeCheck(json, 'age', 'number')).toThrow(
			'age is missing',
		)
	})

	it('should throw an error if the field is not of the correct type', () => {
		const json = { age: 'twenty' }
		expect(() => missingAndTypeCheck(json, 'age', 'number')).toThrow(
			'age must be a number',
		)
	})

	it('should not throw an error if the field is present and of the correct type', () => {
		const json = { age: 20 }
		expect(() => missingAndTypeCheck(json, 'age', 'number')).not.toThrow()
	})

	it('should throw an error if the field is present but of the wrong type (string expected)', () => {
		const json = { name: 123 }
		expect(() => missingAndTypeCheck(json, 'name', 'string')).toThrow(
			'name must be a string',
		)
	})

	it('should throw an error if the field is present but of the wrong type (boolean expected)', () => {
		const json = { isActive: 'true' }
		expect(() => missingAndTypeCheck(json, 'isActive', 'boolean')).toThrow(
			'isActive must be a boolean',
		)
	})

	it('should not throw an error if the field is present and of the correct type (boolean)', () => {
		const json = { isActive: true }
		expect(() => missingAndTypeCheck(json, 'isActive', 'boolean')).not.toThrow()
	})
})

describe('extractCacheFields', () => {
	it('should extract specified fields from the JSON object', () => {
		const json = {
			field1: 'value1',
			field2: 'value2',
			field3: 'value3',
		}
		const cacheFields = ['field1', 'field3']
		const result = extractCacheFields(json, cacheFields)
		expect(result).toEqual({
			field1: 'value1',
			field3: 'value3',
		})
	})

	it('should return an empty object if no fields match', () => {
		const json = {
			field1: 'value1',
			field2: 'value2',
		}
		const cacheFields = ['field3', 'field4']
		const result = extractCacheFields(json, cacheFields)
		expect(result).toEqual({})
	})

	it('should ignore fields that are not present in the JSON object', () => {
		const json = {
			field1: 'value1',
			field2: 'value2',
		}
		const cacheFields = ['field1', 'field3']
		const result = extractCacheFields(json, cacheFields)
		expect(result).toEqual({
			field1: 'value1',
		})
	})

	it('should handle an empty JSON object', () => {
		const json = {}
		const cacheFields = ['field1', 'field2']
		const result = extractCacheFields(json, cacheFields)
		expect(result).toEqual({})
	})

	it('should handle an empty cacheFields array', () => {
		const json = {
			field1: 'value1',
			field2: 'value2',
		}
		const cacheFields: string[] = []
		const result = extractCacheFields(json, cacheFields)
		expect(result).toEqual({})
	})
})
