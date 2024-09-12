import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { MapEntry, Page } from '../internal/page.js'
import {
	type PageGroup,
	createPageGroup,
	getFreePage,
	mergePageMaps,
	sequenceNo,
} from '../internal/pagegroup.js'
import { createOptions } from '../internal/state.js'
import { Vol } from './helpers.js'

describe('createPageGroup', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should create a new page group', async () => {
		Vol.from({
			'./file1.json': '',
			'./file2.json': '',
		})

		const options = createOptions({
			dirPath: Vol.path('./'),
		})
		const group = await createPageGroup(Vol.path('./'), '.json', options)

		expect(group).toMatchObject({
			idMap: new Map(),
			ridMap: new Map(),
			extension: '.json',
			lastIdx: 0,
			dirPath: Vol.path('./'),
			maxPageSize: options.maxPageSize,
			maxStaleBytes: options.maxPageSize * options.staleDataThreshold,
			dataSyncDelay: options.dataSyncDelay,
			close: expect.any(Function),
		})

		expect(group.pages).toHaveLength(2)
	})
})

describe('sequenceNo', () => {
	const ridMap = new Map<string, Partial<MapEntry>>()
	ridMap.set('1', { _seq: 10, _rid: 1, pid: 'page1' })
	ridMap.set('2', { _seq: 20, _rid: 2, pid: 'page2' })
	ridMap.set('3', { _seq: 15, _rid: 3, pid: 'page1' })

	const pg = { ridMap } as unknown as PageGroup

	it('should return the maximum sequence number for the entire page group', () => {
		const result = sequenceNo(pg, Math.max, '_seq')
		expect(result).toBe(20)
	})

	it('should return the minimum sequence number for the entire page group', () => {
		const result = sequenceNo(pg, Math.min, '_seq')
		expect(result).toBe(10)
	})

	it('should return the maximum sequence number for a specific page', () => {
		const result = sequenceNo(pg, Math.max, '_seq', 'page1')
		expect(result).toBe(15)
	})

	it('should return the minimum sequence number for a specific page', () => {
		const result = sequenceNo(pg, Math.min, '_seq', 'page1')
		expect(result).toBe(10)
	})

	it('should return the maximum rid for the entire page group', () => {
		const result = sequenceNo(pg, Math.max, '_rid')
		expect(result).toBe(3)
	})

	it('should return the minimum rid for the entire page group', () => {
		const result = sequenceNo(pg, Math.min, '_rid')
		expect(result).toBe(1)
	})

	it('should return the maximum rid for a specific page', () => {
		const result = sequenceNo(pg, Math.max, '_rid', 'page1')
		expect(result).toBe(3)
	})

	it('should return the minimum rid for a specific page', () => {
		const result = sequenceNo(pg, Math.min, '_rid', 'page1')
		expect(result).toBe(1)
	})
})

describe('getFreePage', () => {
	it('should return the first free page', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('1')
		expect(page?.pageId).toEqual('1')
		expect(pg.lastIdx).toEqual(0)
	})

	it('should return the first unlocked page', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('2')
		expect(page?.pageId).toEqual('2')
		expect(pg.lastIdx).toEqual(1)
	})

	it('should return the page with size less than maxPageSize', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 150 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('2')
		expect(page?.pageId).toEqual('2')
		expect(pg.lastIdx).toEqual(1)
	})

	it('should return the next unlocked page', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: true, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('3')
		expect(page?.pageId).toEqual('3')
		expect(pg.lastIdx).toEqual(2)
	})

	it('should create a new page if no page is available', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: true, size: 50 } as Page,
			{ pageId: '3', locked: true, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0, dirPath: '' } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(page).toBeUndefined()
		expect(pid).toBeDefined()
		// lastIdx will remain unchanged as it will be set by appendToFreePage
		expect(pg.lastIdx).toEqual(0)
	})

	it('should return the first free page if lastIdx is negative', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: -1 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('1')
		expect(page?.pageId).toEqual('1')
		expect(pg.lastIdx).toEqual(0)
	})

	it('should return the first free page if lastIdx is out of bounds', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 10 } as PageGroup
		const [pid, page] = getFreePage(pg)
		expect(pid).toEqual('1')
		expect(page?.pageId).toEqual('1')
		expect(pg.lastIdx).toEqual(0)
	})
})

describe('mergePageMaps', () => {
	it('should merge two page maps', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		target.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })

		const map1 = new Map<string, MapEntry>()
		map1.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })
		map1.set('4', { pageId: '4', offset: 0, size: 0, _seq: 4 })

		mergePageMaps(target, map1)
		expect(target.size).toBe(4)
		expect(target.get('1')).toMatchObject({
			pageId: '1',
			offset: 0,
			size: 0,
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			pageId: '2',
			offset: 0,
			size: 0,
			_seq: 2,
		})
		expect(target.get('3')).toMatchObject({
			pageId: '3',
			offset: 0,
			size: 0,
			_seq: 3,
		})
		expect(target.get('4')).toMatchObject({
			pageId: '4',
			offset: 0,
			size: 0,
			_seq: 4,
		})
	})

	it('should merge multiple page maps', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		target.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })

		const map1 = new Map<string, MapEntry>()
		map1.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })
		map1.set('4', { pageId: '4', offset: 0, size: 0, _seq: 4 })

		const map2 = new Map<string, MapEntry>()
		map2.set('5', { pageId: '5', offset: 0, size: 0, _seq: 5 })
		map2.set('6', { pageId: '6', offset: 0, size: 0, _seq: 6 })

		mergePageMaps(target, map1, map2)
		expect(target.size).toBe(6)
		expect(target.get('1')).toMatchObject({
			pageId: '1',
			offset: 0,
			size: 0,
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			pageId: '2',
			offset: 0,
			size: 0,
			_seq: 2,
		})
		expect(target.get('3')).toMatchObject({
			pageId: '3',
			offset: 0,
			size: 0,
			_seq: 3,
		})
		expect(target.get('4')).toMatchObject({
			pageId: '4',
			offset: 0,
			size: 0,
			_seq: 4,
		})
		expect(target.get('5')).toMatchObject({
			pageId: '5',
			offset: 0,
			size: 0,
			_seq: 5,
		})
		expect(target.get('6')).toMatchObject({
			pageId: '6',
			offset: 0,
			size: 0,
			_seq: 6,
		})
	})

	it('should not overwrite existing entries', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		target.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })

		const map1 = new Map<string, MapEntry>()
		map1.set('2', { pageId: '2', offset: 0, size: 0, _seq: 3 })
		map1.set('3', { pageId: '3', offset: 0, size: 0, _seq: 4 })

		mergePageMaps(target, map1)
		expect(target.size).toBe(3)
		expect(target.get('1')).toMatchObject({
			pageId: '1',
			offset: 0,
			size: 0,
			_seq: 1,
		})
		expect(target.get('2')).toMatchObject({
			pageId: '2',
			offset: 0,
			size: 0,
			_seq: 3,
		})
		expect(target.get('3')).toMatchObject({
			pageId: '3',
			offset: 0,
			size: 0,
			_seq: 4,
		})
	})

	it('should overwrite existing entries with same sequence number', () => {
		const target = new Map<string, MapEntry>()
		target.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		target.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })

		const map1 = new Map<string, MapEntry>()
		map1.set('2', { pageId: '2', offset: 10, size: 0, _seq: 2 })
		map1.set('3', { pageId: '3', offset: 0, size: 0, _seq: 4 })

		mergePageMaps(target, map1)
		expect(target.size).toBe(3)

		expect(target.get('1')).toMatchObject({
			pageId: '1',
			offset: 0,
			size: 0,
			_seq: 1,
		})

		expect(target.get('2')).toMatchObject({
			pageId: '2',
			offset: 10,
			size: 0,
			_seq: 2,
		})

		expect(target.get('3')).toMatchObject({
			pageId: '3',
			offset: 0,
			size: 0,
			_seq: 4,
		})
	})
})
