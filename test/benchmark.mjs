import { JsDb } from '../dist/index.js'
import { deleteTempDir, getTempDir } from './helpers.mjs'

const db = new JsDb({ dirPath: getTempDir() })
await db.open()

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
]
function printPerf(perfName, count) {
	const perf = performance.measure(
		perfName,
		`${perfName}-start`,
		`${perfName}-end`,
	)
	const totalDurationInSeconds = perf.duration / 1000 // Convert milliseconds to seconds
	const entriesPerSecond = count / totalDurationInSeconds
	console.log(
		`✔  ${perfName} per second`.padEnd(40, '.'),
		entriesPerSecond.toFixed(3),
	)
}

const recordCount = 50000
performance.mark('add-records-start')
for (let i = 0; i < recordCount; i++) {
	//  we don't want to have a static record as V8 is some how optimizing it
	await db.set(i.toString(), records[i % records.length])
}
performance.mark('add-records-end')
printPerf('add-records', recordCount)

performance.mark('get-records-start')
for (let i = 0; i < recordCount; i++) {
	await db.get(i.toString())
}
performance.mark('get-records-end')
printPerf('get-records', recordCount)

performance.mark('exists-records-start')
for (let i = 0; i < recordCount; i++) {
	db.exists(i.toString())
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

deleteTempDir(db.options.dirPath)
