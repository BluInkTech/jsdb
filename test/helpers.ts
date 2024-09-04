import {
	mkdirSync,
	readdirSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs'
import os from 'node:os'
import path from 'node:path'

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
	// Create files and folders in the temp directory based on the data
	from(data: Record<string, string | Record<string, string>>, dir?: string) {
		if (!dir) {
			Vol.rootDir = getTempDir()
		}
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
		if (!Vol.rootDir) {
			Vol.rootDir = getTempDir()
		}
		return path.join(Vol.rootDir, ...paths)
	},
}
