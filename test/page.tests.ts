import { existsSync, readFileSync } from 'node:fs'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import {
	type MapEntry,
	compactPage,
	extractCacheFields,
	missingAndTypeCheck,
	openOrCreatePageFile,
	readLines,
	readPageFile,
	writeValue,
} from '../internal/page.js'
import { Vol, words } from './helpers.js'

describe('compactPage', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should compact a page file', async () => {
		const file = Vol.path('./0.page')
		const page = await openOrCreatePageFile(file)
		const value = Buffer.from('{"id":"1","_seq":10}\n')
		await writeValue(page, value, 0)
		const value2 = Buffer.from('{"id":"2","_seq":20}\n')
		await writeValue(page, value2, 0)
		const value3 = Buffer.from('{"id":"3","_seq":30}\n')
		await writeValue(page, value3, 0)

		// add the values to the map
		const map = new Map<string, MapEntry>()
		map.set('1', {
			_seq: 10,
			pid: '0.page',
		})
		map.set('2', {
			_seq: 20,
			pid: '0.page',
		})
		map.set('3', {
			_seq: 30,
			pid: '0.page',
		})

		const result = await readPageFile(file)
		expect(result.size).toBe(3)

		const pages = [page]
		// compact the page
		await compactPage(map, pages, page)

		// after compaction the 0.page file should be renamed to a 0.page.old file
		expect(existsSync(file)).toBe(false)
		expect(existsSync(Vol.path('./0.page.old'))).toBe(true)

		// pages should not have 0.page
		expect(pages.findIndex((x) => x.pageId === '0.page')).toBe(-1)

		// there should be a new page file in pages
		expect(pages.length).toBe(1)
		const newPageId = pages[0].pageId

		// the map should be updated with the new pageId
		for (const [_, entry] of map.entries()) {
			expect(entry.pageId).toBe(newPageId)
		}

		// the new page file should have the data
		const newPageData = readFileSync(Vol.path(`./${newPageId}`))
		expect(newPageData.toString()).toBe(
			'{"id":"1","_seq":10}\n{"id":"2","_seq":20}\n{"id":"3","_seq":30}\n',
		)
	})

	it('should filter entries based on filterSeqNo', async () => {
		const file = Vol.path('./0.page')
		const page = await openOrCreatePageFile(file)
		const value = Buffer.from('{"id":"1","_seq":10}\n')
		await writeValue(page, value, 0)
		const value2 = Buffer.from('{"id":"2","_seq":20}\n')
		await writeValue(page, value2, 0)
		const value3 = Buffer.from('{"id":"3","_seq":30}\n')
		await writeValue(page, value3, 0)

		// add the values to the map
		const map = new Map<string, MapEntry>()
		map.set('1', {
			_oid: 1,
			_rid: 1,
			_seq: 10,
			id: '1',
			pid: '0.page',
			record: '{"id":"1","_seq":10,"_oid":1,"_rid":1}',
		})
		map.set('2', {
			_oid: 1,
			_rid: 2,
			_seq: 20,
			id: '2',
			pid: '0.page',
			record: '{"id":"2","_seq":20,"_oid":1,"_rid":2}',
		})
		map.set('3', {
			_oid: 1,
			_rid: 3,
			_seq: 30,
			id: '3',
			pid: '0.page',
			record: '{"id":"3","_seq":30,"_oid":1,"_rid":3}',
		})

		const pages = [page]
		// compact the page
		await compactPage(map, pages, page, 20)

		const newPageId = pages[0].pageId

		// the map should be updated with the new pageId
		for (const [_, entry] of map.entries()) {
			expect(entry.pid).toBe(newPageId)
		}

		// the new page file should have the data
		const newPageData = readFileSync(Vol.path(`./${newPageId}`))
		expect(newPageData.toString()).toBe(
			`${map['1'].record}\n${map['2'].record}\n`,
		)
	})

	it('should not compact if the page is locked', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		page.locked = true
		const map = new Map<string, MapEntry>()
		const pages = [page]
		expect(compactPage(map, pages, page)).rejects.toThrow(
			'Page is already locked',
		)
	})
})
