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

type LineInfo = { line: string; offset: number; size: number; lineNo: number }
describe('readLines', () => {
	beforeEach(() => {
		Vol.reset()
	})

	afterAll(() => {
		Vol.reset()
	})

	it('should read a file line by line', async () => {
		Vol.from({
			test: '1234567890\n1234567890\n1234567890\n1234\n',
		})

		const filePath = Vol.path('test')
		const result = new Array<LineInfo>()
		//  try with a buffer slightly larger than the average line length
		await readLines(
			filePath,
			(line, offset, size, lineNo) => {
				result.push({ line: line.toString(), offset, size, lineNo })
			},
			12,
		)
		expect(result).toEqual([
			{ line: '1234567890', offset: 0, size: 10, lineNo: 1 },
			{ line: '1234567890', offset: 11, size: 10, lineNo: 2 },
			{ line: '1234567890', offset: 22, size: 10, lineNo: 3 },
			{ line: '1234', offset: 33, size: 4, lineNo: 4 },
		])

		//  try with a buffer slightly smaller than the average line length
		result.length = 0
		await readLines(
			filePath,
			(line, offset, size, lineNo) => {
				result.push({ line: line.toString(), offset, size, lineNo })
			},
			7,
		)
		expect(result).toEqual([
			{ line: '1234567890', offset: 0, size: 10, lineNo: 1 },
			{ line: '1234567890', offset: 11, size: 10, lineNo: 2 },
			{ line: '1234567890', offset: 22, size: 10, lineNo: 3 },
			{ line: '1234', offset: 33, size: 4, lineNo: 4 },
		])
	})

	it('unterminated last line is ignored', async () => {
		Vol.from({
			test: '1234567890\n1234567890\n1234567890\n1234',
		})

		const filePath = Vol.path('test')
		const result = new Array<LineInfo>()
		await readLines(
			filePath,
			(line, offset, size, lineNo) => {
				result.push({ line: line.toString(), offset, size, lineNo })
			},
			12,
		)
		expect(result).toEqual([
			{ line: '1234567890', offset: 0, size: 10, lineNo: 1 },
			{ line: '1234567890', offset: 11, size: 10, lineNo: 2 },
			{ line: '1234567890', offset: 22, size: 10, lineNo: 3 },
		])
	})

	it('empty lines throw error', async () => {
		Vol.from({
			test: '1234567890\n1234567890\n1234567890\n\n1234\n',
		})

		const filePath = Vol.path('test')
		expect(readLines(filePath, () => {}, 12)).rejects.toThrow(
			`Empty line in file:${filePath}`,
		)
	})

	it('non existing file will throw error', async () => {
		const filePath = './test1'
		expect(readLines(filePath, () => {}, 12)).rejects.toThrow(
			"ENOENT: no such file or directory, open './test1'",
		)
	})

	it('offset with unicode characters are correct', async () => {
		const fileContent = words.join('\n').concat('\n')
		const fileContentBuffer = Buffer.from(fileContent)

		Vol.from({
			'0.page': fileContent,
		})
		let i = 0
		await readLines(Vol.path('0.page'), (buffer, offset, size) => {
			const line = buffer.toString()
			expect(line).toEqual(words[i])
			expect(fileContentBuffer.indexOf(buffer)).toEqual(offset)
			expect(buffer).toEqual(fileContentBuffer.subarray(offset, offset + size))
			i++
		})
	})

	it('read a JSON newline file with unicode character', async () => {
		const fileContent = words
			.map((word, i) =>
				JSON.stringify({
					id: i.toString(),
					name: word,
					color: 'yellow',
					_seq: i,
				}),
			)
			.join('\n')
			.concat('\n')

		const fileContentBuffer = Buffer.from(fileContent)
		Vol.from({
			'0.page': fileContent,
		})
		let i = 0
		await readLines(Vol.path('0.page'), (buffer, offset, size) => {
			const line = buffer.toString()
			expect(JSON.parse(line)).toEqual({
				id: i.toString(),
				name: words[i],
				color: 'yellow',
				_seq: i,
			})

			expect(fileContentBuffer.indexOf(buffer)).toEqual(offset)
			expect(buffer).toEqual(fileContentBuffer.subarray(offset, offset + size))
			i++
		})
	})

	it('break character can be escaped', async () => {
		Vol.from({
			test: '12345\\n67890\n1234\n',
		})

		const filePath = Vol.path('test')
		const result = new Array<LineInfo>()
		await readLines(
			filePath,
			(line, offset, size, lineNo) => {
				result.push({ line: line.toString(), offset, size, lineNo })
			},
			12,
		)
		expect(result).toEqual([
			{ line: '12345\\n67890', offset: 0, size: 12, lineNo: 1 },
			{ line: '1234', offset: 13, size: 4, lineNo: 2 },
		])
	})
})

describe('readPageFile', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should read a page file', async () => {
		Vol.from({
			'./0.page': `{"id":"1000","_seq":1,"_rid":1,"_oid":1}
{"id":"1001","_seq":2,"_rid":2,"_oid":1}
{"id":"1002","_seq":3,"_rid":3,"_oid":1}
{"id":"1000","_seq":4,"_rid":4,"_oid":1}\n`,
		})

		const result = await readPageFile(Vol.path('./0.page'))
		expect(result).toEqual(
			new Map([
				[
					'1000',
					{
						id: '1000',
						_oid: 1,
						_seq: 4,
						_rid: 4,
						pid: '0.page',
						record: '{"id":"1000","_seq":4,"_rid":4,"_oid":1}',
					},
				],
				[
					'1001',
					{
						id: '1001',
						_oid: 1,
						_seq: 2,
						_rid: 2,
						pid: '0.page',
						record: '{"id":"1001","_seq":2,"_rid":2,"_oid":1}',
					},
				],
				[
					'1002',
					{
						id: '1002',
						_oid: 1,
						_seq: 3,
						_rid: 3,
						pid: '0.page',
						record: '{"id":"1002","_seq":3,"_rid":3,"_oid":1}',
					},
				],
			]),
		)
	})

	it('should throw error for invalid JSON entries', async () => {
		Vol.from({
			'./test': `{"id":"1000","_seq":1,"_rid":1,"_oid":1}\n{"id":"1001","_seq":2\n`,
		})

		const filePath = Vol.path('./test')
		expect(readPageFile(filePath)).rejects.toThrowError(
			`Invalid JSON entry in ${Vol.path('/test')} at lineNo:2`,
		)
	})

	it('should throw error for non existing file', async () => {
		expect(readPageFile(Vol.path('./test.page'))).rejects.toThrow(
			`ENOENT: no such file or directory, open '${Vol.path('./test.page')}'`,
		)
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

describe('openOrCreatePageFile', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should create a new page file', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.fileName).toBe(Vol.path('./0.page'))
		expect(page.pageId).toBe('0.page')
		expect(page.size).toBe(0)
		expect(page.locked).toBe(false)
		expect(page.closed).toBe(false)

		// check that the file is created
		expect(existsSync(Vol.path('./0.page'))).toBe(true)
	})

	it('should open an existing page file', async () => {
		Vol.from({
			'./0.page': '1234567890\n1234567890\n1234567890\n1234\n',
		})

		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.pageId).toBe('0.page')
		expect(page.size).toBe(38)
	})

	it('should throw error for invalid file path', async () => {
		expect(openOrCreatePageFile('')).rejects.toThrow('ENOENT')
	})

	it('should close the page', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.closed).toBe(false)
		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.closed).toBe(true)

		// check that the data
		expect(existsSync(Vol.path('./0.page'))).toBe(true)
		const data = readFileSync(Vol.path('./0.page'))
		expect(data.toString()).toBe('1234567890\n')
	})

	it('should flush the page', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.closed).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.flush()
		expect(page.closed).toBe(false)

		// check that the data
		expect(existsSync(Vol.path('./0.page'))).toBe(true)
		const data = readFileSync(Vol.path('./0.page'))
		expect(data.toString()).toBe('1234567890\n')
	})

	it('should ignore flush if the file is closed', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.closed).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.closed).toBe(true)

		// check that the data
		expect(existsSync(Vol.path('./0.page'))).toBe(true)
		const data = readFileSync(Vol.path('./0.page'))
		expect(data.toString()).toBe('1234567890\n')

		// flush should not throw error
		expect(page.flush()).resolves.toBeUndefined()
	})

	it('should ignore close if the file is closed', async () => {
		const page = await openOrCreatePageFile(Vol.path('./0.page'))
		expect(page.closed).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.closed).toBe(true)

		// check that the data
		expect(existsSync(Vol.path('./0.page'))).toBe(true)
		const data = readFileSync(Vol.path('./0.page'))
		expect(data.toString()).toBe('1234567890\n')

		// close should not throw error
		expect(page.close()).resolves.toBeUndefined()
	})
})

describe('writeValue', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should write a value to a page immediately (sync 0)', async () => {
		const file = Vol.path('./0.page')
		const page = await openOrCreatePageFile(file)
		const value = Buffer.from('123\n')

		const written = await writeValue(page, value, 0)
		expect(written).toBe(4)

		const data = readFileSync(file)
		expect(data.toString()).toBe('123\n')

		// write another value
		const value2 = Buffer.from('456\n')
		const written2 = await writeValue(page, value2, 0)
		expect(written2).toBe(4)

		const data2 = readFileSync(file)
		expect(data2.toString()).toBe('123\n456\n')
	})

	it('should write a value to a page with debounce (sync 100)', async () => {
		const file = Vol.path('./0.page')
		const page = await openOrCreatePageFile(file)
		const value = Buffer.from('123\n')
		const written = await writeValue(page, value, 100)
		expect(written).toBe(4)

		// wait for the debounce to complete
		await new Promise((resolve) => setTimeout(resolve, 200))

		const data = readFileSync(file)
		expect(data.toString()).toBe('123\n')

		// write another value
		const value2 = Buffer.from('456\n')
		const written2 = await writeValue(page, value2, 100)
		expect(written2).toBe(4)
		await new Promise((resolve) => setTimeout(resolve, 200))

		const data2 = readFileSync(file)
		expect(data2.toString()).toBe('123\n456\n')
	})
})

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
