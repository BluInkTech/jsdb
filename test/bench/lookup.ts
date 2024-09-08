import { assert } from 'node:console'
import { Benchmark } from '../helpers'

const items = 100_000

function binarySearch<T>(arr: T[], target: T) {
	let left = 0
	let right = arr.length - 1
	while (left <= right) {
		const mid = left + Math.floor((right - left) / 2)
		if (arr[mid] === target) return mid
		if (arr[mid] < target) left = mid + 1
		else right = mid - 1
	}
	return -1
}

function numberKeys() {
	const arr = Array.from({ length: items }, (_, i) => i)
	const set = new Set(arr.map((num) => num))
	const map = new Map(arr.map((num) => [num, true]))
	const obj = Object.assign({}, ...arr.map((num) => ({ [num]: true })))

	Benchmark.add('Number Array - includes', () => {
		arr.includes(100)
		arr.includes(5000)
		arr.includes(9000)
	})

	Benchmark.add('Number Array - binarySearch', () => {
		binarySearch(arr, 100)
		binarySearch(arr, 5000)
		binarySearch(arr, 9000)
	})

	Benchmark.add('Number Object - has', () => {
		obj[100]
		obj[5000]
		obj[9000]
	})

	Benchmark.add('Number Set - has', () => {
		set.has(100)
		set.has(5000)
		set.has(9000)
	})

	Benchmark.add('Number Map - has', () => {
		map.has(100)
		map.has(5000)
		map.has(9000)
	})

	Benchmark.run(`Number Lookup Performance ${items} elements`, 1_000_000)
}

function stringKeys() {
	const arrStr = Array.from({ length: items }, (_, i) => `${i}a`)
	const setStr = new Set(arrStr.map((num) => num))
	const mapStr = new Map(arrStr.map((num) => [num, true]))
	const objStr = Object.assign({}, ...arrStr.map((num) => ({ [num]: true })))

	Benchmark.add('String Array - includes', () => {
		arrStr.includes('100a')
		arrStr.includes('5000a')
		arrStr.includes('9000a')
	})

	Benchmark.add('String Array - binarySearch', () => {
		binarySearch(arrStr, '100a')
		binarySearch(arrStr, '5000a')
		binarySearch(arrStr, '9000a')
	})

	Benchmark.add('String Object - has', () => {
		objStr['100a']
		objStr['5000a']
		objStr['9000a']
	})

	Benchmark.add('String Set - has', () => {
		setStr.has('100a')
		setStr.has('5000a')
		setStr.has('9000a')
	})

	Benchmark.add('String Map - has', () => {
		mapStr.has('100a')
		mapStr.has('5000a')
		mapStr.has('9000a')
	})

	Benchmark.run(`String Lookup Performance ${items} elements`, 1_000_000)
}

function iteration() {
	const arr = Array.from({ length: items }, (_, i) => i)
	const set = new Set(arr.map((num) => num))
	const map = new Map(arr.map((num) => [num, true]))
	const obj = Object.assign({}, ...arr.map((num) => ({ [num]: true })))
	const mapKeys = Array.from(map.keys())
	Benchmark.add('Number Array - for of', () => {
		for (const num of arr) {
			num
		}
	})

	Benchmark.add('Number Array - for loop', () => {
		const length = arr.length
		for (let i = 0; i < length; i++) {
			arr[i]
		}
	})

	Benchmark.add('Number Set - for of', () => {
		for (const num of set) {
			num
		}
	})

	Benchmark.add('Number Map - for of keys', () => {
		for (const num of map.keys()) {
			num
		}
	})

	Benchmark.add('Number Map - for of', () => {
		for (const num of map) {
			num
		}
	})

	Benchmark.add('Number Map - cached keys', () => {
		const length = mapKeys.length
		for (let i = 0; i < length; i++) {
			mapKeys[i]
		}
	})

	Benchmark.add('Number Object - for in', () => {
		for (const num in obj) {
			num
		}
	})

	Benchmark.add('Number Object - for of keys', () => {
		for (const num of Object.keys(obj)) {
			num
		}
	})

	Benchmark.run(`Iteration Performance ${items} elements`, 1000)
}

// all tests should move all the items from position 9000 to 9001. It should
// also remove the last element from the array. The length should be the same.
function splice() {
	const arr = Array.from({ length: items }, (_, i) => i)
	const buffer = new Uint32Array(arr)

	Benchmark.add('Number Array - copywithin', () => {
		arr.copyWithin(9001, 9000, items - 1)
		assert(arr.length === items)
	})

	Benchmark.add('Number Array - splice', () => {
		arr.splice(9000, 0, 1)
		assert(arr.length === items + 1)
		arr.length--
		assert(arr.length === items)
	})

	Benchmark.add('Number TypedArray - set', () => {
		buffer.set(buffer.subarray(9000, items - 1), 9001)
		assert(buffer.length === items)
	})

	Benchmark.add('Number TypedArray - copywithin', () => {
		buffer.copyWithin(9001, 9000, items - 1)
		assert(buffer.length === items)
	})

	Benchmark.run(`Splice Performance ${items} elements`, 1000)
}

numberKeys()
stringKeys()
iteration()
splice()
