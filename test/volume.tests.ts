import { readFileSync, readSync, readdirSync } from 'node:fs'
import path from 'node:path'
import {
	afterAll,
	afterEach,
	beforeAll,
	describe,
	expect,
	it,
	vi,
} from 'vitest'
import { type JsDb, openDb } from '../index.js'
import { getTempDir, printDirStats, sleep, words } from './helpers.js'

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
const dirPath = getTempDir()
describe(
	'High volume tests',
	{
		timeout: 120000,
	},
	() => {
		let db: JsDb
		beforeAll(async () => {
			db = await openDb({ dirPath, dataSyncDelay: 100 })
		})

		afterEach(({ task }) => {
			if (task.result?.state !== 'pass') {
				// print the content of the file
				const files = readdirSync(dirPath)
				for (const file of files) {
					const f = readFileSync(path.join(dirPath, file))
					console.log(file)
					console.log(f.toString())
				}
			}
		})

		it('add entry and check previous entry', async () => {
			for (let i = 0; i < entries; i++) {
				await db.set(i.toString(), {
					id: i.toString(),
					word: words[i % 100],
					sentence: sentences[i % 100],
				})

				if (i > 1) {
					// Get the previous entry. We want to ensure that there is no
					// out of order writes. This logic will stress the read/write path
					// enough to catch any issues.
					const entry1 = await db.get((i - 1).toString())
					expect(entry1).toStrictEqual({
						_seq: i,
						id: (i - 1).toString(),
						word: words[(i - 1) % 100],
						sentence: sentences[(i - 1) % 100],
					})

					const entry2 = await db.get((i - 2).toString())
					expect(entry2).toStrictEqual({
						_seq: i - 1,
						id: (i - 2).toString(),
						word: words[(i - 2) % 100],
						sentence: sentences[(i - 2) % 100],
					})
				}
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
