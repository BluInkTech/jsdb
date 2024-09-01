import { describe, expect, it } from 'vitest'
import { createOptions } from '../internal/state'

describe('create option tests', () => {
	it('valid dir path is required', () => {
		expect(() => createOptions({ dirPath: '' })).toThrow('dirPath is required')
	})

	it('max page size must be at least 1024 KB', () => {
		expect(() => createOptions({ dirPath: 'test', maxPageSize: 1023 })).toThrow(
			'maxPageSize must be at least 1024 KB',
		)
	})

	it('max page size must be a multiple of 1024', () => {
		expect(() => createOptions({ dirPath: 'test', maxPageSize: 1025 })).toThrow(
			'maxPageSize must be at least 1024 KB',
		)
	})

	it('data sync delay is set', () => {
		const opts = createOptions({ dirPath: 'test', dataSyncDelay: 100 })
		expect(opts.dataSyncDelay).toBe(100)
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
		const opts = createOptions({ dirPath: 'test', comapctDelay: 100 })
		expect(opts.comapctDelay).toBe(100)
	})

	it('cached fields are set', () => {
		const opts = createOptions({ dirPath: 'test', cachedFields: ['test'] })
		expect(opts.cachedFields).toEqual(['test'])
	})

	it('default options', () => {
		const opts = createOptions({ dirPath: 'test' })
		expect(opts.maxPageSize).toBe(1024 * 1024 * 8)
		expect(opts.dataSyncDelay).toBe(1000)
		expect(opts.staleDataThreshold).toBe(0.1)
		expect(opts.comapctDelay).toBe(1000 * 60 * 60 * 24)
		expect(opts.cachedFields).toEqual([])
	})

	it('custom options', () => {
		const opts = createOptions({
			dirPath: 'test',
			maxPageSize: 1024 * 1024 * 16,
			dataSyncDelay: 2000,
			staleDataThreshold: 0.2,
			comapctDelay: 1000 * 60 * 60 * 12,
			cachedFields: ['test'],
		})
		expect(opts.maxPageSize).toBe(1024 * 1024 * 16)
		expect(opts.dataSyncDelay).toBe(2000)
		expect(opts.staleDataThreshold).toBe(0.2)
		expect(opts.comapctDelay).toBe(1000 * 60 * 60 * 12)
		expect(opts.cachedFields).toEqual(['test'])
	})
})
