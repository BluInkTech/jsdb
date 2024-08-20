import { mkdirSync, rmSync } from 'node:fs'

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
	'🍎apple',
	'苹果',
	'яблоко',
	'μήλο',
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
]

export const getTempDir = () => {
	const tempDir = `./temp/${Math.random().toString(36).substring(7)}`
	mkdirSync(tempDir, { recursive: true })
	return tempDir
}

export const deleteTempDir = (dirPath: string) => {
	rmSync(dirPath, { recursive: true, force: true })
}
