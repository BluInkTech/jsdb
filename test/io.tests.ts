import fs from 'node:fs'
import { vol } from 'memfs'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ensureDir, getFilesWithExtension } from '../internal/io'

vi.mock('node:fs')
vi.mock('node:fs/promises')

describe('getFilesWithExtension', () => {
	beforeEach(() => {
		vol.reset()
	})

	it('should get files with extension', async () => {
		vol.fromJSON({
			'file1.txt': Buffer.from('content'),
			'file2.txt': Buffer.from('content'),
			'file3.md': Buffer.from('content'),
		})

		const files = await getFilesWithExtension('./', '.txt')
		expect(files).toEqual(['file1.txt', 'file2.txt'])
	})
})

describe('ensureDir', () => {
	beforeEach(() => {
		vol.reset()
	})

	it('should create directory', async () => {
		await ensureDir('./test')
		expect(fs.existsSync('./test')).toBe(true)
	})

	it('should not create directory if it already exists', async () => {
		await ensureDir('./test')
		await ensureDir('./test')
		expect(fs.existsSync('./test')).toBe(true)
	})

	it('should throw error if path is not a directory', async () => {
		vol.fromJSON({
			'file.txt': Buffer.from('content'),
		})

		try {
			await ensureDir('./file.txt')
		} catch (err) {
			expect(err.message).toBe('Path is not a directory: ./file.txt')
		}
	})
})
