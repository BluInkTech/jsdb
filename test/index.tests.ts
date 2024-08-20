import assert from 'node:assert/strict'
import { after, before, describe, it } from 'node:test'
import { JsDb } from '../index.js'
import { deleteTempDir, getTempDir, words } from './helpers.js'

describe('High level tests', () => {
	const db = new JsDb({ dirPath: getTempDir() })
	before(async () => {
		await db.open()
	})

	after(async () => {
		await assert.doesNotReject(db.close())
		deleteTempDir(db.options.dirPath)
	})

	it('open a new database', async () => {
		await assert.doesNotReject(db.open())
	})

	it('add a new entry', async () => {
		await db.set('1', { id: '1', name: 'John Doe' })
		const entry = await db.get('1')
		assert.deepEqual(entry, { id: '1', name: 'John Doe' })
	})

	it('update an entry', async () => {
		await db.set('1', { id: '1', name: 'Jane Doe' })
		const entry = await db.get('1')
		assert.deepEqual(entry, { id: '1', name: 'Jane Doe' })
	})

	it('get a non-existing entry', async () => {
		const entry = await db.get('2')
		assert.strictEqual(entry, undefined)
	})

	it('delete an entry', async () => {
		await db.delete('1')
		const entry = await db.get('1')
		assert.strictEqual(entry, undefined)
	})

	it('reload the database after delete', async () => {
		await db.close()
		await db.open()
		const entry = await db.get('1')
		assert.strictEqual(entry, undefined)
	})

	it('add 100 unicode entries', async () => {
		for (let i = 0; i < 100; i++) {
			await db.set(i.toString(), { id: i.toString(), name: words[i] })
		}
	})

	it('get 100 unicode entries', async () => {
		for (let i = 0; i < 100; i++) {
			const entry = await db.get(i.toString())
			assert.deepEqual(entry, { id: i.toString(), name: words[i] })
		}
	})

	it('close and reopen the database', async () => {
		await db.close()
		await db.open()

		// check if the entries are still there
		for (let i = 0; i < 100; i++) {
			const entry = await db.get(i.toString())
			assert.deepEqual(entry, { id: i.toString(), name: words[i] })
		}
	})
})
