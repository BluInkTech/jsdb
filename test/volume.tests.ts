import assert from 'node:assert/strict'
import { after, before, describe, it } from 'node:test'
import { JsDb } from '../index.js'
import { deleteTempDir, getTempDir, words } from './helpers.mjs'

// create random length sentences using the words
const sentences: Array<string> = []
for (let i = 0; i < 100; i++) {
	const length = Math.floor(Math.random() * 10) + 1
	const sentence: Array<string> = []
	for (let j = 0; j < length; j++) {
		sentence.push(words[Math.floor(Math.random() * 100)])
	}
	sentences.push(sentence.join(' '))
}

const entries = 10000

describe('High volume tests', () => {
	const db = new JsDb({ dirPath: getTempDir() })
	before(async () => {
		await assert.doesNotReject(db.open())
	})

	after(async () => {
		await assert.doesNotReject(db.close())
		deleteTempDir(db.options.dirPath)
	})

	it('open a new database', async () => {
		await assert.doesNotReject(db.open())
	})

	it('add entries', async () => {
		for (let i = 0; i < entries; i++) {
			await db.set(i.toString(), { id: i.toString(), word: words[i % 100], sentence: sentences[i % 100] })
		}
	})

	it('get entries', async () => {
		for (let i = 0; i < entries; i++) {
			const entry = await db.get(i.toString())
			assert.deepEqual(entry, { id: i.toString(), word: words[i % 100], sentence: sentences[i % 100] })
		}
	})

	it('close and reopen the database', async () => {
		await db.close()
		await db.open()

		// check if the entries are still there
		for (let i = 0; i < entries; i++) {
			const entry = await db.get(i.toString())
			assert.deepEqual(entry, { id: i.toString(), word: words[i % 100], sentence: sentences[i % 100] })
		}
	})
})
