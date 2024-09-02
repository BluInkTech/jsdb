import { vol } from 'memfs'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
	type MapEntry,
	compactPage,
	openOrCreatePageFile,
	readLines,
	readPageFile,
	readValue,
	writeValue,
} from '../internal/page.js'
import { printDirStats, words } from './helpers.js'

vi.mock('node:fs')
vi.mock('node:fs/promises')

type LineInfo = { line: string; offset: number; size: number; lineNo: number }
describe('readLines', () => {
	beforeEach(() => {
		vol.reset()
	})

	it('should read a file line by line', async () => {
		vol.fromJSON({
			'./test': '1234567890\n1234567890\n1234567890\n1234\n',
		})

		const filePath = './test'
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
		vol.fromJSON({
			'./test': '1234567890\n1234567890\n1234567890\n1234',
		})

		const filePath = './test'
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
		vol.fromJSON({
			'./test': '1234567890\n1234567890\n1234567890\n\n1234\n',
		})

		const filePath = './test'
		expect(readLines(filePath, () => {}, 12)).rejects.toThrow(
			'Empty line in file:./test',
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

		vol.fromJSON({
			'./0.page': fileContent,
		})
		let i = 0
		await readLines('./0.page', (buffer, offset, size) => {
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
		vol.fromJSON({
			'./0.page': fileContent,
		})
		let i = 0
		await readLines('./0.page', (buffer, offset, size) => {
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
		vol.fromJSON({
			'./test': '12345\\n67890\n1234\n',
		})

		const filePath = './test'
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
		vol.reset()
	})

	it('should read a page file', async () => {
		vol.fromJSON({
			'./0.page': `{"id":"1000","_seq":1}\n{"id":"1001","_seq":2}\n{"id":"1002","_seq":3}\n{"id":"1000","_seq":4}\n`,
		})

		const result = await readPageFile('./0.page')
		expect(result).toEqual(
			new Map([
				['1000', { _seq: 4, offset: 69, size: 22, pageId: '0.page' }],
				['1001', { _seq: 2, offset: 23, size: 22, pageId: '0.page' }],
				['1002', { _seq: 3, offset: 46, size: 22, pageId: '0.page' }],
			]),
		)
	})

	it('should read a page file with cache fields', async () => {
		vol.fromJSON({
			'./0.page': `{"id":"1000","_seq":1,"name":"John"}\n{"id":"1001","_seq":2,"name":"Doe"}\n{"id":"1002","_seq":3,"name":"Jane"}\n{"id":"1000","_seq":4,"name":"Smith"}\n`,
		})

		const result = await readPageFile('./0.page', ['name'])
		expect(result).toEqual(
			new Map([
				[
					'1000',
					{
						_seq: 4,
						offset: 110,
						size: 37,
						pageId: '0.page',
						cache: { name: 'Smith' },
					},
				],
				[
					'1001',
					{
						_seq: 2,
						offset: 37,
						size: 35,
						pageId: '0.page',
						cache: { name: 'Doe' },
					},
				],
				[
					'1002',
					{
						_seq: 3,
						offset: 73,
						size: 36,
						pageId: '0.page',
						cache: { name: 'Jane' },
					},
				],
			]),
		)
	})

	it('should throw error for invalid JSON entries', async () => {
		vol.fromJSON({
			'./test': `{"id":"1000","_seq":1}\n{"id":"1001","_seq":2\n`,
		})

		const filePath = './test'
		expect(readPageFile(filePath)).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:2',
				cause:
					"Expected ',' or '}' after property value in JSON at position 21",
			}),
		)
	})

	it('should throw error for invalid id field', async () => {
		vol.fromJSON({
			'./test': `{"id":1000,"_seq":1}\n`,
		})

		expect(readPageFile('./test')).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:1',
				cause: 'id must be a string',
			}),
		)
	})

	it('should throw error for invalid _seq field', async () => {
		vol.fromJSON({
			'./test': `{"id":"1000","_seq":"1"}\n`,
		})

		expect(readPageFile('./test')).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:1',
				cause: '_seq must be a number',
			}),
		)
	})

	it('should throw error for missing id field', async () => {
		vol.fromJSON({
			'./test': `{"_seq":1}\n`,
		})

		expect(readPageFile('./test')).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:1',
				cause: 'id and _seq are required fields',
			}),
		)
	})

	it('should throw error for missing _seq field', async () => {
		vol.fromJSON({
			'./test': `{"id":"1000"}\n`,
		})

		expect(readPageFile('./test')).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:1',
				cause: 'id and _seq are required fields',
			}),
		)
	})

	it('should throw error for non existing file', async () => {
		expect(readPageFile('./test')).rejects.toThrow(
			"ENOENT: no such file or directory, open './test'",
		)
	})

	it('round trip test', async () => {
		const fileContent = words
			.map((word, i) =>
				JSON.stringify({ id: i.toString(), name: word, _seq: i }),
			)
			.join('\n')
			.concat('\n')

		vol.fromJSON({
			'./0.page': fileContent,
		})

		const result = await readPageFile('./0.page')
		expect(result.size).toBe(words.length)

		const contentBuffer = Buffer.from(fileContent)
		// check that the reported offsets and sizes are correct
		for (const [id, entry] of result) {
			const line = contentBuffer
				.subarray(entry.offset, entry.offset + entry.size)
				.toString()
			expect(line).toContain(
				JSON.stringify({ id: id, name: words[entry._seq], _seq: entry._seq }),
			)
		}

		const page = await openOrCreatePageFile('./0.page')
		// should be able to read the data from the file based on the generated offset and size
		let i = 0
		for (const [id, entry] of result) {
			const value = await readValue(page, entry.offset, entry.size)
			expect(value).toEqual({
				id: id,
				name: words[i],
				_seq: i,
			})
			i++
		}
	})
})

describe('openOrCreatePageFile', () => {
	beforeEach(() => {
		vol.reset()
		vol.fromJSON({
			'./empty.temp': '',
		})
	})

	it('should create a new page file', async () => {
		const page = await openOrCreatePageFile('./0.page')
		expect(page.fileName).toBe('./0.page')
		expect(page.pageId).toBe('0.page')
		expect(page.size).toBe(0)
		expect(page.locked).toBe(false)
		expect(page.closed).toBe(false)
		expect(page.hasClosed()).toBe(false)

		// check that the file is created
		expect(vol.existsSync('./0.page')).toBe(true)
	})

	it('should open an existing page file', async () => {
		vol.fromJSON({
			'./0.page': '1234567890\n1234567890\n1234567890\n1234\n',
		})

		const page = await openOrCreatePageFile('./0.page')
		expect(page.pageId).toBe('0.page')
		expect(page.size).toBe(38)
	})

	it('should throw error for invalid file path', async () => {
		expect(openOrCreatePageFile('')).rejects.toThrow('EISDIR')
	})

	it('should close the page', async () => {
		const page = await openOrCreatePageFile('./0.page')
		expect(page.hasClosed()).toBe(false)
		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.hasClosed()).toBe(true)

		// check that the data
		expect(vol.existsSync('./0.page')).toBe(true)
		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('1234567890\n')
	})

	it('should flush the page', async () => {
		const page = await openOrCreatePageFile('./0.page')
		expect(page.hasClosed()).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.flush()
		expect(page.hasClosed()).toBe(false)

		// check that the data
		expect(vol.existsSync('./0.page')).toBe(true)
		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('1234567890\n')
	})

	it('should ignore flush if the file is closed', async () => {
		const page = await openOrCreatePageFile('./0.page')
		expect(page.hasClosed()).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.hasClosed()).toBe(true)

		// check that the data
		expect(vol.existsSync('./0.page')).toBe(true)
		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('1234567890\n')

		// flush should not throw error
		expect(page.flush()).resolves.toBeUndefined()
	})

	it('should ignore close if the file is closed', async () => {
		const page = await openOrCreatePageFile('./0.page')
		expect(page.hasClosed()).toBe(false)

		// write some data to the file
		const b = Buffer.from('1234567890\n')
		await page.handle.write(b, 0, b.length, -1)
		await page.close()
		expect(page.hasClosed()).toBe(true)

		// check that the data
		expect(vol.existsSync('./0.page')).toBe(true)
		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('1234567890\n')

		// close should not throw error
		expect(page.close()).resolves.toBeUndefined()
	})
})

describe('writeValue', () => {
	beforeEach(() => {
		vol.reset()
		vol.fromJSON({
			'./empty.temp': '',
		})
	})

	it('should write a value to a page immediately (sync 0)', async () => {
		const page = await openOrCreatePageFile('./0.page')
		const value = Buffer.from('123\n')

		const written = await writeValue(page, value, 0)
		expect(written).toBe(4)

		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('123\n')

		// write another value
		const value2 = Buffer.from('456\n')
		const written2 = await writeValue(page, value2, -1)
		expect(written2).toBe(4)

		const data2 = vol.readFileSync('./0.page')
		expect(data2.toString()).toBe('123\n456\n')
	})

	it('should write a value to a page with debounce (sync 100)', async () => {
		const page = await openOrCreatePageFile('./0.page')
		const value = Buffer.from('123\n')
		const written = await writeValue(page, value, 100)
		expect(written).toBe(4)

		// Note: it is difficult to test the below
		// writeValue function writes data to the file handle, and the data is
		// likely being cached by the operating system's file system buffers.
		// The fs.write operation writes data to the file descriptor, but it
		// doesn't necessarily flush the data to disk immediately.
		// The data can still be read from the file descriptor even if it hasn't
		// been flushed to disk.
		// const emptyFile = vol.readFileSync('./0.page')
		// expect(emptyFile.toString()).toBe('')

		// wait for the debounce to complete
		await new Promise((resolve) => setTimeout(resolve, 200))

		const data = vol.readFileSync('./0.page')
		expect(data.toString()).toBe('123\n')

		await new Promise((resolve) => setTimeout(resolve, 200))
		// write another value
		const value2 = Buffer.from('456\n')
		const written2 = await writeValue(page, value2, 100)
		expect(written2).toBe(4)

		const data2 = vol.readFileSync('./0.page')
		expect(data2.toString()).toBe('123\n456\n')
	})
})

describe('compactPage', () => {
	beforeEach(() => {
		vol.reset()
		vol.fromJSON({
			'./empty.temp': '',
		})
	})

	it('should compact a page file', async () => {
		const page = await openOrCreatePageFile('./0.page')
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
			offset: 0,
			size: value.byteLength,
			pageId: '0.page',
		})
		map.set('2', {
			_seq: 20,
			offset: value.byteLength,
			size: value2.byteLength,
			pageId: '0.page',
		})
		map.set('3', {
			_seq: 30,
			offset: value.byteLength + value2.byteLength,
			size: value3.byteLength,
			pageId: '0.page',
		})

		const result = await readPageFile('./0.page')
		expect(result.size).toBe(3)

		const pages = [page]
		// compact the page
		await compactPage(map, pages, page)

		// after compaction the 0.page file should be renamed to a 0.page.old file
		expect(vol.existsSync('./0.page')).toBe(false)
		expect(vol.existsSync('./0.page.old')).toBe(true)

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
		const newPageData = vol.readFileSync(`./${newPageId}`)
		expect(newPageData.toString()).toBe(
			'{"id":"1","_seq":10}\n{"id":"2","_seq":20}\n{"id":"3","_seq":30}\n',
		)
	})

	it('should filter entries based on filterSeqNo', async () => {
		const page = await openOrCreatePageFile('./0.page')
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
			offset: 0,
			size: value.byteLength,
			pageId: '0.page',
		})
		map.set('2', {
			_seq: 20,
			offset: value.byteLength,
			size: value2.byteLength,
			pageId: '0.page',
		})
		map.set('3', {
			_seq: 30,
			offset: value.byteLength + value2.byteLength,
			size: value3.byteLength,
			pageId: '0.page',
		})

		const pages = [page]
		// compact the page
		await compactPage(map, pages, page, 20)

		const newPageId = pages[0].pageId

		// the map should be updated with the new pageId
		for (const [_, entry] of map.entries()) {
			expect(entry.pageId).toBe(newPageId)
		}

		// the new page file should have the data
		const newPageData = vol.readFileSync(`./${newPageId}`)
		expect(newPageData.toString()).toBe(
			'{"id":"2","_seq":20}\n{"id":"3","_seq":30}\n',
		)
	})

	it('should not compact if the page is locked', async () => {
		const page = await openOrCreatePageFile('./0.page')
		page.locked = true
		const map = new Map<string, MapEntry>()
		const pages = [page]
		expect(compactPage(map, pages, page)).rejects.toThrow(
			'Page is already locked',
		)
	})
})
