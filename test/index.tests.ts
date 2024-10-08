import { beforeEach, describe, expect, it, vi } from 'vitest'
import { openDb } from '../index.js'
import { Vol, testRecords, words } from './helpers.js'

describe('JsDB public interface tests', () => {
	beforeEach(() => {
		Vol.reset()
	})

	it('add a new entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		const sut = db.set('1', { id: '1', name: 'John Doe' })
		const entry = db.get('1')
		expect(entry).toEqual(sut)
		await db.close()
	})

	it('get an entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = db.get('1')
		expect(entry?.id).toBe('1')
		expect(entry?.name).toBe('Jane Doe')
		expect(entry?._oid).toBe(1)
		expect(entry?._seq).toBe(1)
		expect(entry?._rid).toBe(1)
		await db.close()
	})

	it('write and retrieve multiple entries', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', ...testRecords[1] })
		db.set('2', { id: '2', ...testRecords[2] })
		expect(db.get('1')).toStrictEqual({
			id: '1',
			_oid: 1,
			_rid: 1,
			_seq: 1,
			...testRecords[1],
		})
		db.set('3', { id: '3', ...testRecords[3] })
		expect(db.get('2')).toStrictEqual({
			id: '2',
			_oid: 1,
			_rid: 2,
			_seq: 2,
			...testRecords[2],
		})
		expect(db.get('3')).toStrictEqual({
			id: '3',
			_oid: 1,
			_seq: 3,
			_rid: 3,
			...testRecords[3],
		})
		await db.close()
	})

	it('update an entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', name: 'John Doe' })
		const sut = db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = db.get('1')
		expect(entry).toEqual(sut)
		expect(entry?._seq).toBe(2)
		await db.close()
	})

	it('has an entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', name: 'John Doe' })
		const has = db.has('1')
		expect(has).to.true
		await db.close()
	})

	it('get a non-existing entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		const entry = db.get('2')
		expect(entry).toBeUndefined
		await db.close()
	})

	it('delete an entry', async () => {
		const db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', name: 'John Doe' })
		let has = db.has('1')
		expect(has).to.true
		db.delete('1')
		const entry = db.get('1')
		expect(entry).toBeUndefined
		has = db.has('1')
		expect(has).to.false
		await db.close()
	})

	it('reload the database after delete', async () => {
		let db = await openDb({ dirPath: Vol.path('/') })
		db.set('1', { id: '1', name: 'John Doe' })
		db.delete('1')
		await db.close()

		db = await openDb({ dirPath: Vol.path('/') })
		const entry = db.get('1')
		expect(entry).toBeUndefined
		await db.close()
	})

	it('test with unicode values', async () => {
		let db = await openDb({ dirPath: Vol.path('/') })
		for (let i = 0; i < 100; i++) {
			db.set(i.toString(), { id: i.toString(), name: words[i] })
		}
		await db.close()

		// reload and check if the entries are still there
		db = await openDb({ dirPath: Vol.path('/') })
		for (let i = 0; i < 100; i++) {
			const entry = db.get(i.toString())
			expect(entry).toBeDefined
			expect(entry?.id).toBe(i.toString())
			expect(entry?.name).toBe(words[i])
			expect(entry?._seq).toBe(i + 1)
		}
		await db.close()
	})
})
