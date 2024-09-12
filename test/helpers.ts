import {
	mkdirSync,
	readdirSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { type PerformanceMeasure, performance } from 'node:perf_hooks'

/* cSpell:disable */
//  100 words with unicode characters
export const words = [
	'hello🌍',
	'你好',
	'привет',
	'γεια',
	'👋world',
	'世界',
	'мир',
	'κόσμος',
	'🍎apple',
	'苹果',
	'яблоко',
	'μήλο',
	'🐱cat',
	'猫',
	'кошка',
	'γάτα',
	'🐶dog',
	'狗',
	'собака',
	'σκύλος',
	'🌞sun',
	'太阳',
	'солнце',
	'ήλιος',
	'🌙moon',
	'月亮',
	'луна',
	'σελήνη',
	'⭐star',
	'星星',
	'звезда',
	'αστέρι',
	'🌈rainbow',
	'彩虹',
	'радуга',
	'ουράνιο τόξο',
	'🌺flower',
	'花',
	'цветок',
	'λουλούδι',
	'🍁maple',
	'枫叶',
	'клен',
	'σφενδάμι',
	'🍄mushroom',
	'蘑菇',
	'гриб',
	'μανιτάρι',
	'🌳tree',
	'树',
	'дерево',
	'δέντρο',
	'🌊wave',
	'波浪',
	'волна',
	'κύμα',
	'🍇grape',
	'葡萄',
	'виноград',
	'σταφύλι',
	'🍉watermelon',
	'西瓜',
	'арбуз',
	'καρπούζι',
	'🍌banana',
	'香蕉',
	'банан',
	'μπανάνα',
	'🍍pineapple',
	'菠萝',
	'ананас',
	'ανανάς',
	'🍐pear',
	'梨',
	'груша',
	'αχλάδι',
	'🍊orange',
	'橙子',
	'апельсин',
	'πορτοκάλι',
	'🍋lemon',
	'柠檬',
	'лимон',
	'λεμόνι',
	'🍓strawberry',
	'草莓',
	'клубника',
	'φράουλα',
	'🍅tomato',
	'番茄',
	'томат',
	'ντομάτα',
	'🍆eggplant',
	'茄子',
	'баклажан',
	'μελιτζάνα',
	'🥦broccoli',
	'西兰花',
	'брокколи',
	'μπρόκολο',
	'🥬lettuce',
	'生菜',
	'салат',
	'μαρούλι',
	'🥒cucumber',
	'黄瓜',
	'огурец',
	'αγγούρι',
	'🥕carrot',
	'胡萝卜',
	'морковь',
	'καρότο',
	'🌽corn',
	'玉米',
	'кукуруза',
	'καλαμπόκι',
	'🌶️pepper',
	'辣椒',
	'перец',
	'πιπεριά',
	'🥔potato',
	'土豆',
	'картофель',
	'πατάτα',
	'🍞bread',
	'面包',
	'хлеб',
	'ψωμί',
	'🥐croissant',
	'羊角面包',
	'круассан',
	'κρουασάν',
	'🥖baguette',
	'法棍面包',
	'багет',
	'μπαγκέτα',
	'🥨pretzel',
	'नमस्ते',
	'مرحبا',
]

export const testRecords = [
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

export const getTempDir = () => {
	const tempDir = path.join(
		os.tmpdir(),
		'jsdb-test',
		Math.random().toString(36).substring(7),
	)
	mkdirSync(tempDir, { recursive: true })
	return tempDir
}

export const deleteTempDir = (dirPath) => {
	rmSync(dirPath, { recursive: true, force: true })
}

export const printDirStats = (dir) => {
	console.log()
	console.log(`Directory: ${dir}`)
	// print directory stats and file list along with sizes
	for (const file of readdirSync(dir)) {
		const stats = statSync(path.join(dir, file))
		console.log(
			`${file.padEnd(30, '.')} ${(stats.size / 1024).toLocaleString()} Kb`,
		)
	}
}

export const sleep = (ms: number) =>
	new Promise((resolve) => setTimeout(resolve, ms))

const sut = {
	folder1: {
		'file1.txt': 'file1 content',
		folder2: {
			'file2.txt': 'file2 content',
		},
	},
}

export const Vol = {
	rootDir: '',
	createRootDir() {
		if (!Vol.rootDir) {
			Vol.rootDir = getTempDir()
		}
	},
	// Create files and folders in the temp directory based on the data
	from(data: Record<string, string | Record<string, string>>, dir?: string) {
		Vol.createRootDir()
		for (const [key, value] of Object.entries(data)) {
			// check if value is a string
			if (value !== undefined && typeof value === 'string') {
				// create the file with the content
				const filePath = path.join(Vol.rootDir, key)
				mkdirSync(path.dirname(filePath), { recursive: true })
				writeFileSync(filePath, value)
				continue
			}
			const filePath = path.join(Vol.rootDir, key)
			if (typeof value === 'object') {
				mkdirSync(filePath, { recursive: true })
				Vol.from(value, Vol.rootDir)
			} else {
				mkdirSync(path.dirname(filePath), { recursive: true })
				mkdirSync(filePath, { recursive: true })
			}
		}
	},
	reset() {
		if (Vol.rootDir) {
			rmSync(Vol.rootDir, { recursive: true, force: true })
			Vol.rootDir = ''
		}
	},
	path(...paths: string[]) {
		Vol.createRootDir()
		return path.join(Vol.rootDir, ...paths)
	},
}

type BenchmarkOptions = { count?: number }
export const Benchmark = {
	tests: [] as { name: string; fn: () => void; options?: BenchmarkOptions }[],
	results: [] as { name: string; perf: PerformanceMeasure; heap: number }[],

	title(title: string) {
		console.log()
		console.log(title)
		console.log('-'.repeat(title.length))
	},

	add(name: string, fn: () => void, options?: BenchmarkOptions) {
		Benchmark.tests.push({ name, fn, options })
	},

	run(title: string, count: number) {
		for (const test of Benchmark.tests) {
			global.gc?.()
			const memStart = process.memoryUsage().heapUsed
			performance.mark(`${test.name}-start`)
			// capture the memory usage before running the test
			const actualCount = test.options?.count || count
			for (let i = 0; i < actualCount; i++) {
				test.fn()
			}
			performance.mark(`${test.name}-end`)
			const memEnd = process.memoryUsage().heapUsed
			// console.log('Finished running test:', test.name)
			Benchmark.results.push({
				name: test.name,
				perf: performance.measure(
					test.name,
					`${test.name}-start`,
					`${test.name}-end`,
				),
				heap: memEnd - memStart,
			})
		}

		console.log()
		Benchmark.title(`${title} (x${count.toLocaleString()})`)
		Benchmark.printPerf(count)
		// clear the tests
		Benchmark.tests = []
		Benchmark.results = []
	},

	printPerf(count: number) {
		Benchmark.results.sort((a, b) => a.perf.duration - b.perf.duration)
		const bestDuration = Benchmark.results[0].perf.duration

		for (const r of Benchmark.results) {
			const perf = performance.measure(
				r.name,
				`${r.name}-start`,
				`${r.name}-end`,
			)
			const totalDurationInSeconds = perf.duration / 1000 // Convert milliseconds to seconds
			const entriesPerSecond = count / totalDurationInSeconds
			let timePerOperation = perf.duration / count // time in milliseconds

			// create a user friendly unit for timePerOperation in ms, μs or ns
			let timeUnit = 'ms'
			if (timePerOperation < 0.001) {
				timePerOperation = timePerOperation * 1000 * 1000
				timeUnit = 'ns'
			} else if (timePerOperation < 1) {
				timePerOperation = timePerOperation * 1000
				timeUnit = 'μs'
			} else if (timePerOperation > 1000) {
				timePerOperation = timePerOperation / 1000
				timeUnit = 's'
			} else {
				timeUnit = 'ms'
			}

			// Calculate how many times slower the worst test is compared to this one
			const xTimes = 100 - (bestDuration / perf.duration) * 100

			console.log(
				// biome-ignore lint/style/useTemplate: <explanation>
				`✔  ${r.name}`.padEnd(40, '.') +
					Math.trunc(entriesPerSecond).toLocaleString().padStart(20, '.') +
					' ops/s' +
					timePerOperation.toFixed(2).toLocaleString().padStart(15, '.') +
					' ' +
					timeUnit +
					'/op' +
					xTimes.toFixed(2).padStart(20, '.') +
					' % slower',
			)
		}
	},
}
