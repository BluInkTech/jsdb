import { vol } from 'memfs'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { openDb } from '../index.js'
import { printDirStats, testRecords, words } from './helpers.js'

vi.mock('node:fs')
vi.mock('node:fs/promises')

describe('JsDB public interface tests', () => {
	beforeEach(() => {
		vol.reset()
	})

	it('add a new entry', async () => {
		const db = await openDb({ dirPath: '/' })
		const sut = await db.set('1', { id: '1', name: 'John Doe' })
		const entry = await db.get('1')
		expect(entry).toEqual(sut)
		await db.close()
	})

	it('get an entry', async () => {
		const db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = await db.get('1')
		expect(entry?.id).toBe('1')
		expect(entry?.name).toBe('Jane Doe')
		expect(entry?._seq).toBe(1)
		await db.close()
	})

	it('write and retrieve multiple entries', async () => {
		const db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', ...testRecords[1] })
		await db.set('2', { id: '2', ...testRecords[2] })
		await db.set('3', { id: '3', ...testRecords[3] })
		expect(await db.get('1')).toStrictEqual({
			id: '1',
			_seq: 1,
			...testRecords[1],
		})
		expect(await db.get('2')).toStrictEqual({
			id: '2',
			_seq: 2,
			...testRecords[2],
		})
		expect(await db.get('3')).toStrictEqual({
			id: '3',
			_seq: 3,
			...testRecords[3],
		})
		await db.close()
		printDirStats('/')
	})

	it('update an entry', async () => {
		const db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', name: 'John Doe' })
		const sut = await db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = await db.get('1')
		expect(entry).toEqual(sut)
		expect(entry?._seq).toBe(2)
		await db.close()
	})

	it('has an entry', async () => {
		const db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', name: 'John Doe' })
		const has = db.has('1')
		expect(has).to.true
		await db.close()
	})

	it('get a non-existing entry', async () => {
		const db = await openDb({ dirPath: '/' })
		const entry = await db.get('2')
		expect(entry).toBeUndefined
		await db.close()
	})

	it('delete an entry', async () => {
		const db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', name: 'John Doe' })
		let has = db.has('1')
		expect(has).to.true
		await db.delete('1')
		const entry = await db.get('1')
		expect(entry).toBeUndefined
		has = db.has('1')
		expect(has).to.false
		await db.close()
	})

	it('reload the database after delete', async () => {
		let db = await openDb({ dirPath: '/' })
		await db.set('1', { id: '1', name: 'John Doe' })
		await db.delete('1')

		await db.close()
		db = await openDb({ dirPath: '/' })
		const entry = await db.get('1')
		expect(entry).toBeUndefined
		await db.close()
	})

	it('test with unicode values', async () => {
		let db = await openDb({ dirPath: '/' })
		for (let i = 0; i < 100; i++) {
			await db.set(i.toString(), { id: i.toString(), name: words[i] })
		}
		await db.close()

		// reload and check if the entries are still there
		db = await openDb({ dirPath: '/' })
		for (let i = 0; i < 100; i++) {
			const entry = await db.get(i.toString())
			expect(entry).toBeDefined
			expect(entry?.id).toBe(i.toString())
			expect(entry?.name).toBe(words[i])
			expect(entry?._seq).toBe(i + 1)
		}
		await db.close()
	})
})
