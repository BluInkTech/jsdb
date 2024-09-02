import { readdirSync, statSync } from 'node:fs'
import path from 'node:path'
import { vol } from 'memfs'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { type JsDb, openDb } from '../index.js'
import { getTempDir, printDirStats, words } from './helpers.js'

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

const entries = 5000
vi.mock('node:fs')
vi.mock('node:fs/promises')
vol.reset()
const dirPath = '/' //getTempDir()
describe(
	'High volume tests',
	{
		timeout: 60000,
	},
	() => {
		let db: JsDb
		beforeAll(async () => {
			db = await openDb({ dirPath })
		})

		it('add entries and check', async () => {
			for (let i = 0; i < entries; i++) {
				await db.set(i.toString(), {
					id: i.toString(),
					word: words[i % 100],
					sentence: sentences[i % 100],
				})

				// get the entry
				const entry = await db.get(i.toString())
				expect(entry).toStrictEqual({
					_seq: i + 1,
					id: i.toString(),
					word: words[i % 100],
					sentence: sentences[i % 100],
				})
			}
		})

		it('get entries', async () => {
			for (let i = 0; i < entries; i++) {
				const entry = await db.get(i.toString())
				expect(entry).toStrictEqual({
					_seq: i + 1,
					id: i.toString(),
					word: words[i % 100],
					sentence: sentences[i % 100],
				})
			}
		})

		it('close and reopen the database', async () => {
			await db.close()
			db = await openDb({ dirPath })

			// check if the entries are still there
			for (let i = 0; i < entries; i++) {
				const entry = await db.get(i.toString())
				expect(entry).toStrictEqual({
					_seq: i + 1,
					id: i.toString(),
					word: words[i % 100],
					sentence: sentences[i % 100],
				})
			}
		})

		it('delete entries', async () => {
			for (let i = 0; i < entries; i++) {
				await db.delete(i.toString())
				expect(await db.get(i.toString())).toBeUndefined()
			}
		})

		it('close and reopen the database to verify deleted entries', async () => {
			await db.close()
			db = await openDb({ dirPath })

			for (let i = 0; i < entries; i++) {
				await db.delete(i.toString())
				expect(await db.get(i.toString())).toBeUndefined()
			}
		})

		it('print directory stats', async () => {
			await db.close()
		})
	},
)
