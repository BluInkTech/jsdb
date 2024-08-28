import { vol } from 'memfs'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
	openOrCreatePageFile,
	readLines,
	readPageFile,
	readValue,
} from '../../internal/page'
import { words } from '../helpers.mjs'

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
				JSON.stringify({ id: i.toString(), name: word, _seq: i }),
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

	it('should read a delete log page file', async () => {
		vol.fromJSON({
			'./test': `{"id":"1000","_seq":1}\n{"id":"1001","_seq":2}\n{"id":"1002","_seq":3}\n{"id":"1000","_seq":4}\n`,
		})

		const result = await readPageFile('./test', 'delete')
		expect(result).toEqual(
			new Map([
				['1000', 4],
				['1001', 2],
				['1002', 3],
			]),
		)
	})

	it('should read an append log page file', async () => {
		vol.fromJSON({
			'./0.page': `{"id":"1000","_seq":1}\n{"id":"1001","_seq":2}\n{"id":"1002","_seq":3}\n{"id":"1000","_seq":4}\n`,
		})

		const result = await readPageFile('./0.page', 'append')
		expect(result).toEqual(
			new Map([
				['1000', { _seq: 4, offset: 69, size: 22, pageId: '0.page' }],
				['1001', { _seq: 2, offset: 23, size: 22, pageId: '0.page' }],
				['1002', { _seq: 3, offset: 46, size: 22, pageId: '0.page' }],
			]),
		)
	})

	it('should read an append log page file with cache fields', async () => {
		vol.fromJSON({
			'./0.page': `{"id":"1000","_seq":1,"name":"John"}\n{"id":"1001","_seq":2,"name":"Doe"}\n{"id":"1002","_seq":3,"name":"Jane"}\n{"id":"1000","_seq":4,"name":"Smith"}\n`,
		})

		const result = await readPageFile('./0.page', 'append', ['name'])
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
		expect(readPageFile(filePath, 'delete')).rejects.toThrowError(
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

		expect(readPageFile('./test', 'delete')).rejects.toThrowError(
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

		expect(readPageFile('./test', 'delete')).rejects.toThrowError(
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

		expect(readPageFile('./test', 'delete')).rejects.toThrowError(
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

		expect(readPageFile('./test', 'delete')).rejects.toThrowError(
			expect.objectContaining({
				message: 'Invalid JSON entry in ./test at lineNo:1',
				cause: 'id and _seq are required fields',
			}),
		)
	})

	it('should throw error for non existing file', async () => {
		expect(readPageFile('./test', 'delete')).rejects.toThrow(
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

		const result = await readPageFile('./0.page', 'append', 0)
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
