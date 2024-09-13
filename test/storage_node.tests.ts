import fs from 'node:fs'
import fsp from 'node:fs/promises'
import {
	afterAll,
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	vi,
} from 'vitest'
import {
	appendToFile,
	ensureDir,
	ensureFile,
	getFilesWithExtension,
	readLines,
} from '../internal/storage_node'
import sinon from 'sinon'

import { Vol, words } from './helpers'

describe('getFilesWithExtension', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should get files with extension', async () => {
		Vol.from({
			'file1.txt': 'content',
			'file2.txt': 'content',
			'file3.md': 'content',
		})

		const files = await getFilesWithExtension(Vol.path('./'), '.txt')
		expect(files).toEqual(['file1.txt', 'file2.txt'])
	})
})

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
		const result = new Array<[string, number]>()

		//  try with a buffer slightly larger than the average line length
		for await (const line of readLines(Vol.rootDir, 'test', undefined, 12)) {
			result.push(line)
		}
		expect(result).toEqual([
			['1234567890', 1],
			['1234567890', 2],
			['1234567890', 3],
			['1234', 4],
		])

		//  try with a buffer slightly smaller than the average line length
		result.length = 0
		for await (const line of readLines(Vol.rootDir, 'test', undefined, 7)) {
			result.push(line)
		}
		expect(result).toEqual([
			['1234567890', 1],
			['1234567890', 2],
			['1234567890', 3],
			['1234', 4],
		])
	})

	it('unterminated last line is ignored', async () => {
		Vol.from({
			test: '1234567890\n1234567890\n1234567890\n1234',
		})

		const result = new Array<[string, number]>()
		for await (const line of readLines(Vol.rootDir, 'test')) {
			result.push(line)
		}
		expect(result).toEqual([
			['1234567890', 1],
			['1234567890', 2],
			['1234567890', 3],
		])
	})

	it('empty lines throw error', async () => {
		Vol.from({
			test: '1234567890\n1234567890\n1234567890\n\n1234\n',
		})

		const filePath = Vol.path('test')
		expect(async () => {
			for await (const _ of readLines(Vol.rootDir, 'test')) {
			}
		}).rejects.toThrow(`Empty line in file:${filePath}`)
	})

	it('non existing file will throw error', async () => {
		expect(async () => {
			for await (const _ of readLines(Vol.rootDir, 'test1')) {
			}
		}).rejects.toThrow("ENOENT: no such file or directory, open 'test1'")
	})

	it('content with unicode characters is read correctly', async () => {
		const fileContent = words.join('\n').concat('\n')

		Vol.from({
			'0.page': fileContent,
		})
		let i = 0
		const result = new Array<[string, number]>()
		for await (const line of readLines(Vol.rootDir, '0.page')) {
			expect(line).toEqual([words[i], i + 1])
			i++
		}
	})

	it('break character can be escaped', async () => {
		Vol.from({
			test: '12345\\n67890\n1234\n',
		})

		const result = new Array<[string, number]>()
		for await (const line of readLines(Vol.rootDir, 'test')) {
			result.push(line)
		}
		expect(result).toEqual([
			['12345\\n67890', 1],
			['1234', 2],
		])
	})
})

describe('appendToFile', () => {
	let writeSyncStub: sinon.SinonStub
	let writeStub: sinon.SinonStub
	let fdatasyncSyncStub: sinon.SinonStub

	beforeEach(() => {
		writeSyncStub = sinon.stub(fs, 'writeSync')
		writeStub = sinon.stub(fs, 'write')
		fdatasyncSyncStub = sinon.stub(fs, 'fdatasyncSync')
	})

	afterEach(() => {
		sinon.restore()
	})

	it('should write synchronously if sync is 0', async () => {
		const fd = 1
		const buffer = Buffer.from('test data')
		const sync = 0

		writeSyncStub.returns(buffer.byteLength)

		const bytesWritten = await appendToFile(fd, buffer, sync)

		sinon.assert.calledOnceWithExactly(
			writeSyncStub,
			fd,
			buffer,
			0,
			buffer.byteLength,
		)
		expect(bytesWritten).toBe(buffer.byteLength)
	})

	it('should write asynchronously and sync after delay if sync is greater than 0', async () => {
		const fd = 1
		const buffer = Buffer.from('test data')
		const sync = 1000

		writeStub.yields(null, buffer.byteLength)

		const bytesWritten = await appendToFile(fd, buffer, sync)
		sinon.assert.calledOnceWithExactly(
			writeStub,
			fd,
			buffer,
			0,
			buffer.byteLength,
			null,
			sinon.match.func,
		)
		sinon.assert.calledOnce(fdatasyncSyncStub)
		expect(bytesWritten).toBe(buffer.byteLength)
	})

	it('should handle write error', async () => {
		const fd = 1
		const buffer = Buffer.from('test data')
		const sync = 1000
		const error = new Error('write error')

		writeStub.yields(error)
		await expect(appendToFile(fd, buffer, sync)).rejects.toThrow('write error')
	})

	it('should handle fdatasyncSync error', async () => {
		const fd = 1
		const buffer = Buffer.from('test data')
		const sync = 1000

		writeStub.yields(null, buffer.byteLength)
		fdatasyncSyncStub.throws(new Error('fdatasyncSync error'))

		await expect(appendToFile(fd, buffer, sync)).rejects.toThrow(
			'fdatasyncSync error',
		)
	})

	it('should ignore EBADF error in fdatasyncSync', async () => {
		const fd = 1
		const buffer = Buffer.from('test data')
		const sync = 1000

		writeStub.yields(null, buffer.byteLength)
		fdatasyncSyncStub.throws({ code: 'EBADF' })

		const bytesWritten = await appendToFile(fd, buffer, sync)

		sinon.assert.calledOnce(fdatasyncSyncStub)
		expect(bytesWritten).toBe(buffer.byteLength)
	})
})

describe('ensureFile', () => {
	it('should create a file if it does not exist', async () => {
		const filePath = '/path/to/nonexistent/file.txt'
		const statStub = sinon.stub(fsp, 'stat').rejects({ code: 'ENOENT' })
		const writeFileStub = sinon.stub(fsp, 'writeFile').resolves()

		await ensureFile(filePath)

		expect(writeFileStub.calledWith(filePath, '')).toBe(true)

		statStub.restore()
		writeFileStub.restore()
	})

	it('should not create a file if it already exists', async () => {
		const filePath = '/path/to/existing/file.txt'
		const statStub = sinon.stub(fsp, 'stat').resolves({ isFile: () => true })
		const writeFileStub = sinon.stub(fsp, 'writeFile')

		await ensureFile(filePath)

		expect(writeFileStub.notCalled).toBe(true)

		statStub.restore()
		writeFileStub.restore()
	})

	it('should throw an error if the path exists but is not a file', async () => {
		const filePath = '/path/to/existing/directory'
		const statStub = sinon.stub(fsp, 'stat').resolves({ isFile: () => false })

		await expect(ensureFile(filePath)).rejects.toThrow(
			`Path is not a file: ${filePath}`,
		)

		statStub.restore()
	})

	it('should rethrow any other errors', async () => {
		const filePath = '/path/to/file.txt'
		const error = new Error('Some other error')
		const statStub = sinon.stub(fsp, 'stat').rejects(error)

		await expect(ensureFile(filePath)).rejects.toThrow(error)

		statStub.restore()
	})
})

describe('ensureDir', () => {
	it('should create directory', async () => {
		await ensureDir(Vol.path('./test'))
		expect(fs.existsSync(Vol.path('./test'))).toBe(true)
		Vol.reset()
	})

	it('should not create directory if it already exists', async () => {
		await ensureDir(Vol.path('./test'))
		await ensureDir(Vol.path('./test'))
		expect(fs.existsSync(Vol.path('./test'))).toBe(true)
		Vol.reset()
	})

	it('should throw error if path is not a directory', async () => {
		Vol.from({
			'file.txt': 'content',
		})

		try {
			await ensureDir(Vol.path('file.txt'))
		} catch (err) {
			expect(err.message).toBe(
				`Path is not a directory: ${Vol.path('./file.txt')}`,
			)
		}
		Vol.reset()
	})

	it('should create a directory if it does not exist', async () => {
		const dirPath = '/path/to/nonexistent/directory'
		const statStub = sinon.stub(fsp, 'stat').rejects({ code: 'ENOENT' })
		const mkdirStub = sinon.stub(fsp, 'mkdir').resolves()

		await ensureDir(dirPath)

		expect(mkdirStub.calledWith(dirPath, { recursive: true })).toBe(true)

		statStub.restore()
		mkdirStub.restore()
	})

	it('should not create a directory if it already exists', async () => {
		const dirPath = '/path/to/existing/directory'
		const statStub = sinon
			.stub(fsp, 'stat')
			.resolves({ isDirectory: () => true })
		const mkdirStub = sinon.stub(fsp, 'mkdir')

		await ensureDir(dirPath)

		expect(mkdirStub.notCalled).toBe(true)

		statStub.restore()
		mkdirStub.restore()
	})

	it('should throw an error if the path exists but is not a directory', async () => {
		const dirPath = '/path/to/existing/file'
		const statStub = sinon
			.stub(fsp, 'stat')
			.resolves({ isDirectory: () => false })

		await expect(ensureDir(dirPath)).rejects.toThrow(
			`Path is not a directory: ${dirPath}`,
		)

		statStub.restore()
	})

	it('should rethrow any other errors', async () => {
		const dirPath = '/path/to/directory'
		const error = new Error('Some other error')
		const statStub = sinon.stub(fsp, 'stat').rejects(error)

		await expect(ensureDir(dirPath)).rejects.toThrow(error)

		statStub.restore()
	})
})
