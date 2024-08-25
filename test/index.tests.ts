import { readFileSync } from 'node:fs'
import { vol } from 'memfs'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { JsDb } from '../index.js'
import { words } from './helpers.mjs'

vi.mock('node:fs')
vi.mock('node:fs/promises')

describe('JsDB public interface tests', () => {
	const db = new JsDb({ dirPath: '/' })
	beforeAll(async () => {
		vol.reset()
		await db.open()
	})

	afterAll(async () => {
		expect(db.close()).resolves
	})

	it('add a new entry', async () => {
		const sut = await db.set('1', { id: '1', name: 'John Doe' })
		const entry = await db.get('1')
		expect(entry).toEqual(sut)
	})

	it('update an entry', async () => {
		const sut = await db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = await db.get('1')
		expect(entry).toEqual(sut)
	})

	it('has an entry', () => {
		const has = db.has('1')
		expect(has).to.true
	})

	it('get a non-existing entry', async () => {
		const entry = await db.get('2')
		expect(entry).toBeUndefined
	})

	it('delete an entry', async () => {
		await db.delete('1')
		const entry = await db.get('1')
		expect(entry).toBeUndefined
	})

	it('reload the database after delete', async () => {
		await db.close()
		await db.open()
		const entry = await db.get('1')
		expect(entry).toBeUndefined
	})

	it('add 100 unicode entries', async () => {
		for (let i = 0; i < 100; i++) {
			await db.set(i.toString(), { id: i.toString(), name: words[i] })
		}
	})

	it('get 100 unicode entries', async () => {
		for (let i = 0; i < 100; i++) {
			const entry = await db.get(i.toString())
			expect(entry).toBeDefined
			if (entry === undefined) return // to satisfy TypeScript
			expect(entry.id).toBe(i.toString())
			expect(entry.name).toBe(words[i])
			expect(entry._seq).toBeTruthy
		}
	})

	it('close and reopen the database', async () => {
		await db.close()
		await db.open()

		// check if the entries are still there
		for (let i = 0; i < 100; i++) {
			const entry = await db.get(i.toString())
			expect(entry).toBeDefined
			if (entry === undefined) return // to satisfy TypeScript
			expect(entry.id).toBe(i.toString())
			expect(entry.name).toBe(words[i])
		}
	})
})
