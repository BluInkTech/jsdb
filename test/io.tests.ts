import fs from 'node:fs'
import { beforeEach, describe, expect, it } from 'vitest'
import { ensureDir, getFilesWithExtension } from '../internal/io'
import { Vol } from './helpers'

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
		expect(files).toEqual([Vol.path('file1.txt'), Vol.path('file2.txt')])
	})
})

describe('ensureDir', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('should create directory', async () => {
		await ensureDir(Vol.path('./test'))
		expect(fs.existsSync(Vol.path('./test'))).toBe(true)
	})

	it('should not create directory if it already exists', async () => {
		await ensureDir(Vol.path('./test'))
		await ensureDir(Vol.path('./test'))
		expect(fs.existsSync(Vol.path('./test'))).toBe(true)
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
	})
})
