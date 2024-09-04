import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { MapEntry, Page } from '../internal/page.js'
import {
	type PageGroup,
	createPageGroup,
	getfreePage,
	maxSequenceNo,
	mergePageMaps,
	minSequenceNo,
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
			map: new Map(),
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

describe('maxSequenceNo', () => {
	it('should return the max sequence number', async () => {
		const map = new Map<string, MapEntry>()
		map.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		map.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })
		map.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })

		const pg = { map } as PageGroup
		const maxSeq = maxSequenceNo(pg)
		expect(maxSeq).toBe(3)
	})

	it('should return the max sequence number for a specific page', async () => {
		const map = new Map<string, MapEntry>()
		map.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		map.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })
		map.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })

		const pg = { map } as PageGroup
		const maxSeq = maxSequenceNo(pg, '2')
		expect(maxSeq).toBe(2)
	})
})

describe('minSequenceNo', () => {
	it('should return the min sequence number', async () => {
		const map = new Map<string, MapEntry>()
		map.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		map.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })
		map.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })

		const pg = { map } as PageGroup
		const minSeq = minSequenceNo(pg)
		expect(minSeq).toBe(1)
	})

	it('should return the min sequence number for a specific page', async () => {
		const map = new Map<string, MapEntry>()
		map.set('1', { pageId: '1', offset: 0, size: 0, _seq: 1 })
		map.set('2', { pageId: '2', offset: 0, size: 0, _seq: 2 })
		map.set('3', { pageId: '3', offset: 0, size: 0, _seq: 3 })

		const pg = { map } as PageGroup
		const minSeq = minSequenceNo(pg, '2')
		expect(minSeq).toBe(2)
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
		const page = await getfreePage(pg)
		expect(page.pageId).toEqual('1')
		expect(pg.lastIdx).toEqual(0)
	})

	it('should return the first unlocked page', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const page = await getfreePage(pg)
		expect(page.pageId).toEqual('2')
		expect(pg.lastIdx).toEqual(1)
	})

	it('should return the page with size less than maxPageSize', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 150 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const page = await getfreePage(pg)
		expect(page.pageId).toEqual('2')
		expect(pg.lastIdx).toEqual(1)
	})

	it('should return the next unlocked page', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: true, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0 } as PageGroup
		const page = await getfreePage(pg)
		expect(page.pageId).toEqual('3')
		expect(pg.lastIdx).toEqual(2)
	})

	it('should create a new page if no page is available', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: true, size: 50 } as Page,
			{ pageId: '2', locked: true, size: 50 } as Page,
			{ pageId: '3', locked: true, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 0, dirPath: '' } as PageGroup
		const page = await getfreePage(pg, async (_: string) => {
			return { pageId: '4' } as Page
		})
		expect(page.pageId).toEqual('4')
		expect(pg.pages).toHaveLength(4)
		expect(pg.lastIdx).toEqual(3)
	})

	it('should reset lastIdx if it is out of bounds', async () => {
		const pages: Page[] = [
			{ pageId: '1', locked: false, size: 50 } as Page,
			{ pageId: '2', locked: false, size: 50 } as Page,
			{ pageId: '3', locked: false, size: 50 } as Page,
		]
		const pg = { maxPageSize: 100, pages, lastIdx: 3 } as PageGroup
		const page = await getfreePage(pg)
		expect(page.pageId).toEqual('1')
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
