import { describe, expect, it } from 'vitest'
import { BufferList, bisectLeft } from '../internal/bufferlist'

describe('BufferList tests', () => {
	it('should initialize with a given size', () => {
		const list = new BufferList()
		expect(list.length).toBe(0)
		expect(list.array.length).toBe(1024)
		expect(list.buffer.byteLength).toBe(4096)
	})

	it('should initialize with given elements', () => {
		const list = new BufferList({ init: [1, 2, 3] })
		expect(list.length).toBe(3)
		expect(list.get(0)).toBe(1)
		expect(list.get(1)).toBe(2)
		expect(list.get(2)).toBe(3)
	})

	it('should initialize with 10000 items', () => {
		const itemsCount = 10000
		const list = new BufferList({
			init: Array.from({ length: itemsCount }, (_, i) => i),
		})
		expect(list.length).toBe(itemsCount)
		expect(list.array.length).toBe(itemsCount)
		expect(list.buffer.byteLength).toBe(40000)
	})

	it('should push values correctly', () => {
		const list = new BufferList()
		list.push(1)
		list.push(2)
		expect(list.length).toBe(2)
		expect(list.get(0)).toBe(1)
		expect(list.get(1)).toBe(2)
	})

	it('should resize when pushing beyond initial capacity', () => {
		const list = new BufferList({ size: 4 })
		expect(list.array.length).toBe(1)
		list.push(1)
		list.push(2)
		list.push(3)
		expect(list.length).toBe(3)
		// The length increase is in the increment of 1024
		expect(list.array.length).toBe(1024)
		expect(list.get(2)).toBe(3)
	})

	it('should pop values correctly', () => {
		const list = new BufferList({ init: [1, 2, 3] })
		expect(list.pop()).toBe(3)
		expect(list.length).toBe(2)
		expect(list.pop()).toBe(2)
		expect(list.length).toBe(1)
		expect(list.pop()).toBe(1)
		expect(list.length).toBe(0)
		expect(list.pop()).toBeUndefined()
	})

	it('should get values correctly', () => {
		const list = new BufferList({ init: [1, 2, 3] })
		expect(list.get(0)).toBe(1)
		expect(list.get(1)).toBe(2)
		expect(list.get(2)).toBe(3)
		expect(list.get(3)).toBeUndefined()
	})

	it('should set values correctly', () => {
		const list = new BufferList({ stride: 1, init: [0, 0, 0] })

		list.set(0, 1)
		list.set(1, 2)
		list.set(2, 3)
		expect(list.get(0)).toBe(1)
		expect(list.get(1)).toBe(2)
		expect(list.get(2)).toBe(3)
	})

	it('should throw error when setting out of bounds', () => {
		const list = new BufferList()
		expect(() => list.set(3, 4)).toThrow('Index out of bounds')
	})

	it('should copyWithin correctly', () => {
		const list = new BufferList({ init: [1, 2, 3, 4, 5] })
		list.copyWithin(0, 3)
		expect(list.get(0)).toBe(4)
		expect(list.get(1)).toBe(5)
		expect(list.get(2)).toBe(3)
		expect(list.get(3)).toBe(4)
		expect(list.get(4)).toBe(5)
	})

	describe('setSorted', () => {
		it('should set the value correctly when the length matches the stride', () => {
			const list = new BufferList({ stride: 3 })
			list.setSorted(1, [10, 20])
			const value = list.getSorted(1)
			expect(value).toEqual(new Uint32Array([1, 10, 20]))
		})

		it('should throw an error when the length does not match the stride', () => {
			const list = new BufferList({ stride: 3 })
			expect(() => list.setSorted(1, [10])).toThrow('Invalid value length')
		})

		it('should replace the existing value if the id already exists', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.setSorted(1, [30, 40])
			const value = bufferList.getSorted(1)
			expect(value).toEqual(new Uint32Array([1, 30, 40]))
		})

		it('should insert the value if the id does not exist', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.setSorted(2, [30, 40])
			const value1 = bufferList.getSorted(1)
			const value2 = bufferList.getSorted(2)
			expect(value1).toEqual(new Uint32Array([1, 10, 20]))
			expect(value2).toEqual(new Uint32Array([2, 30, 40]))
		})

		it('should insert the value if the id does not exist (multiple)', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			expect(bufferList.array[0]).toBe(1)
			expect(bufferList.array[1]).toBe(10)
			expect(bufferList.array[2]).toBe(20)
			expect(bufferList.array[3]).toBe(0)

			bufferList.setSorted(3, [50, 60])
			expect(bufferList.array[0]).toBe(1)
			expect(bufferList.array[1]).toBe(10)
			expect(bufferList.array[2]).toBe(20)
			expect(bufferList.array[3]).toBe(3)
			expect(bufferList.array[4]).toBe(50)
			expect(bufferList.array[5]).toBe(60)
			expect(bufferList.array[6]).toBe(0)

			bufferList.setSorted(2, [30, 40])
			expect(bufferList.array[0]).toBe(1)
			expect(bufferList.array[1]).toBe(10)
			expect(bufferList.array[2]).toBe(20)
			expect(bufferList.array[3]).toBe(2)
			expect(bufferList.array[4]).toBe(30)
			expect(bufferList.array[5]).toBe(40)
			expect(bufferList.array[6]).toBe(3)
			expect(bufferList.array[7]).toBe(50)
			expect(bufferList.array[8]).toBe(60)
			expect(bufferList.array[9]).toBe(0)

			const value1 = bufferList.getSorted(1)
			const value2 = bufferList.getSorted(2)
			const value3 = bufferList.getSorted(3)
			expect(value1).toEqual(new Uint32Array([1, 10, 20]))
			expect(value2).toEqual(new Uint32Array([2, 30, 40]))
			expect(value3).toEqual(new Uint32Array([3, 50, 60]))
		})
	})

	describe('getSorted', () => {
		it('should return the value if it exists', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			const value = bufferList.getSorted(1)
			expect(value).toEqual(new Uint32Array([1, 10, 20]))
		})

		it('should return undefined if the value does not exist', () => {
			const bufferList = new BufferList({ stride: 3 })
			const value = bufferList.getSorted(1)
			expect(value).toBeUndefined()
		})
	})

	describe('removeSorted', () => {
		it('should remove the value if it exists', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.removeSorted(1)
			const value = bufferList.getSorted(1)
			expect(value).toBeUndefined()
		})

		it('should do nothing if the value does not exist', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.removeSorted(2)
			const value = bufferList.getSorted(1)
			expect(value).toEqual(new Uint32Array([1, 10, 20]))
		})

		it('should remove the value and shift the rest of the array', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.setSorted(2, [30, 40])
			bufferList.removeSorted(1)
			const value = bufferList.getSorted(1)
			expect(value).toBeUndefined()
			const value2 = bufferList.getSorted(2)
			expect(value2).toEqual(new Uint32Array([2, 30, 40]))
		})

		it('should remove the value and shift the rest of the array (multiple)', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.setSorted(2, [30, 40])
			bufferList.setSorted(3, [50, 60])
			bufferList.removeSorted(2)
			const value = bufferList.getSorted(2)
			expect(value).toBeUndefined()
			const value2 = bufferList.getSorted(3)
			expect(value2).toEqual(new Uint32Array([3, 50, 60]))
		})

		it('should remove the value and shift the rest of the array (end)', () => {
			const bufferList = new BufferList({ stride: 3 })
			bufferList.setSorted(1, [10, 20])
			bufferList.setSorted(2, [30, 40])
			bufferList.removeSorted(2)
			const value = bufferList.getSorted(2)
			expect(value).toBeUndefined()
		})
	})
})

describe('bisectLeft tests', () => {
	it('bisectLeft - 0 case', () => {
		const buffer = new Uint32Array()
		expect(bisectLeft(buffer, 1, 1)).toEqual(0)
	})

	it('bisectLeft - width 2', () => {
		const buffer = new Uint32Array([1, 100, 3, 100])
		expect(bisectLeft(buffer, 0, 2)).toBe(0)
		expect(bisectLeft(buffer, 1, 2)).toBe(0)
		expect(bisectLeft(buffer, 3, 2)).toBe(2)
		expect(bisectLeft(buffer, 2, 2)).toBe(2)
		expect(bisectLeft(buffer, 4, 2)).toBe(4)
	})

	it('bisectLeft - width 3', () => {
		const buffer = new Uint32Array([1, 100, 100, 3, 100, 100])
		expect(bisectLeft(buffer, 0, 3)).toBe(0)
		expect(bisectLeft(buffer, 1, 3)).toBe(0)
		expect(bisectLeft(buffer, 3, 3)).toBe(3)
		expect(bisectLeft(buffer, 2, 3)).toBe(3)
		expect(bisectLeft(buffer, 4, 3)).toBe(6)
	})

	it('bisectLeft - width 4', () => {
		const buffer = new Uint32Array([1, 100, 100, 100, 3, 100, 100, 100])
		expect(bisectLeft(buffer, 0, 4)).toBe(0)
		expect(bisectLeft(buffer, 1, 4)).toBe(0)
		expect(bisectLeft(buffer, 3, 4)).toBe(4)
		expect(bisectLeft(buffer, 2, 4)).toBe(4)
		expect(bisectLeft(buffer, 4, 4)).toBe(8)
	})

	it('bisectLeft - integer array (width 1)', () => {
		const buffer = [1, 3]
		expect(bisectLeft(buffer, 0, 1)).toBe(0)
		expect(bisectLeft(buffer, 1, 1)).toBe(0)
		expect(bisectLeft(buffer, 3, 1)).toBe(1)
		expect(bisectLeft(buffer, 2, 1)).toBe(1)
		expect(bisectLeft(buffer, 4, 1)).toBe(2)
	})

	it('bisectLeft - integer array (width 2)', () => {
		const buffer = [1, 100, 3, 100]
		expect(bisectLeft(buffer, 0, 2)).toBe(0)
		expect(bisectLeft(buffer, 1, 2)).toBe(0)
		expect(bisectLeft(buffer, 3, 2)).toBe(2)
		expect(bisectLeft(buffer, 2, 2)).toBe(2)
		expect(bisectLeft(buffer, 4, 2)).toBe(4)
	})

	it('bisectLeft - object array', () => {
		const buffer = [
			{ key: 1, value: 100 },
			{ key: 3, value: 100 },
		]

		const comparator = (src, id) => {
			if (src.key === id) {
				return 0
			}

			if (src.key > id) {
				return 1
			}

			return -1
		}

		expect(bisectLeft(buffer, 0, 1, comparator)).toBe(0)
		expect(bisectLeft(buffer, 1, 1, comparator)).toBe(0)
		expect(bisectLeft(buffer, 3, 1, comparator)).toBe(1)
		expect(bisectLeft(buffer, 2, 1, comparator)).toBe(1)
		expect(bisectLeft(buffer, 4, 1, comparator)).toBe(2)
	})
})
