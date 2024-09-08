import { rmSync } from 'node:fs'
// import { bench, vi } from 'vitest'
import { type Idable, openDb } from '../index.js'
import { getTempDir, printDirStats } from './helpers.js'

const dir = getTempDir()
let db = await openDb({ dirPath: dir, dataSyncDelay: 1000 })

// create test records
const records = [
	{ name: '🍋lemon', color: 'yellow' },
	{ name: '🍓strawberry', color: 'red' },
	{ name: '🍅tomato', color: 'red' },
	{ name: '🍆eggplant', color: 'purple' },
	{ name: '🥦broccoli', color: 'green' },
	{ name: '🥬lettuce', color: 'green' },
	{ name: '🥒cucumber', color: 'green' },
	{ name: '🥕carrot', color: 'orange' },
	{ name: '🌽corn', color: 'yellow' },
	{ name: '🌶️pepper', color: 'red' },
	{ name: '🥔potato', color: 'brown' },
	{ name: '🍞bread', color: 'brown' },
	{ name: '🥐croissant', color: 'brown' },
	{ name: '🥖baguette', color: 'brown' },
] as unknown as Idable[]
function printPerf(perfName, count) {
	const perf = performance.measure(
		perfName,
		`${perfName}-start`,
		`${perfName}-end`,
	)
	const totalDurationInSeconds = perf.duration / 1000 // Convert milliseconds to seconds
	const entriesPerSecond = count / totalDurationInSeconds
	console.log(
		`✔  ${perfName} per second`.padEnd(40, '.') +
			Math.trunc(entriesPerSecond).toLocaleString().padStart(20, '.'),
	)
}

const recordCount = 1000 // 1_000_000
performance.mark('add-records-start')
// await Promise.all(
// 	Array.from({ length: recordCount }, (_, i) =>
// 		db.set(i.toString(), records[i % records.length]),
// 	),
// )
for (let i = 0; i < recordCount; i++) {
	await db.set(i.toString(), records[i % records.length])
}
performance.mark('add-records-end')
printPerf('add-records', recordCount)

await db.close()
db = await openDb({ dirPath: dir })

performance.mark('get-records-start')
for (let i = 0; i < recordCount; i++) {
	await db.get(i.toString())
}
performance.mark('get-records-end')
printPerf('get-records', recordCount)

performance.mark('exists-records-start')
for (let i = 0; i < recordCount; i++) {
	db.has(i.toString())
}
performance.mark('exists-records-end')
printPerf('exists-records', recordCount)

performance.mark('delete-records-start')
for (let i = 0; i < recordCount; i++) {
	await db.delete(i.toString())
}
performance.mark('delete-records-end')
printPerf('delete-records', recordCount)

await db.close()
printDirStats(dir)

rmSync(dir, { recursive: true, force: true })
