import { describe, it, expect } from 'vitest'
import { BITS_PER_PAGE, Bitset } from '../internal/bitset'

describe('Bitset', () => {
	it('should initialize with length 0', () => {
		const bitset = new Bitset()
		expect(bitset.length).toBe(0)
	})

	it('should resize correctly', () => {
		const bitset = new Bitset()
		bitset.resize(1024)
		expect(bitset.length).toBe(8192)

		bitset.resize(10000)
		expect(bitset.length).toBe(16384)
	})

	it('should set and get bits correctly', () => {
		const bitset = new Bitset()
		bitset.set(10)
		expect(bitset.get(10)).toBe(1)
		expect(bitset.get(11)).toBe(0)
	})

	it('should clear bits correctly', () => {
		const bitset = new Bitset()
		bitset.set(10)
		bitset.clear(10)
		expect(bitset.get(10)).toBe(0)
	})

	it('should clear all bits correctly', () => {
		const bitset = new Bitset()
		bitset.set(10)
		bitset.set(20)
		bitset.clearAll()
		expect(bitset.get(10)).toBe(0)
		expect(bitset.get(20)).toBe(0)
	})

	it('should calculate union correctly', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(10)
		bitset2.set(20)

		bitset1.calculate(bitset2, target, 'union')
		expect(target.get(10)).toBe(1)
		expect(target.get(20)).toBe(1)
	})

	it('should calculate intersection correctly', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(10)
		bitset2.set(10)
		bitset2.set(20)

		bitset1.calculate(bitset2, target, 'intersection')
		expect(target.get(10)).toBe(1)
		expect(target.get(20)).toBe(0)
	})

	it('should calculate difference correctly', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(10)
		bitset1.set(20)
		bitset2.set(20)

		bitset1.calculate(bitset2, target, 'difference')
		expect(target.get(10)).toBe(1)
		expect(target.get(20)).toBe(0)
	})

	it('should iterate over set bits correctly', () => {
		const bitset = new Bitset()
		bitset.set(10)
		bitset.set(20)

		const bits = Array.from(bitset)
		expect(bits).toEqual([10, 20])
	})

	// Additional tests for paging logic
	it('should handle setting bits across multiple pages', () => {
		const bitset = new Bitset()
		bitset.set(BITS_PER_PAGE + 10) // Set a bit in the second page
		expect(bitset.get(BITS_PER_PAGE + 10)).toBe(1)
		expect(bitset.get(10)).toBe(0) // Ensure bit in the first page is not set
	})

	it('should handle clearing bits across multiple pages', () => {
		const bitset = new Bitset()
		bitset.set(BITS_PER_PAGE + 10) // Set a bit in the second page
		bitset.clear(BITS_PER_PAGE + 10) // Clear the bit in the second page
		expect(bitset.get(BITS_PER_PAGE + 10)).toBe(0)
	})

	it('should resize correctly across multiple pages', () => {
		const bitset = new Bitset()
		bitset.resize(BITS_PER_PAGE * 2) // Resize to cover two pages
		expect(bitset.length).toBe(BITS_PER_PAGE * 2)
	})

	it('should calculate union correctly across multiple pages', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(BITS_PER_PAGE + 10)
		bitset2.set(BITS_PER_PAGE + 20)

		bitset1.calculate(bitset2, target, 'union')
		expect(target.get(BITS_PER_PAGE + 10)).toBe(1)
		expect(target.get(BITS_PER_PAGE + 20)).toBe(1)
	})

	it('should calculate intersection correctly across multiple pages', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(BITS_PER_PAGE + 10)
		bitset2.set(BITS_PER_PAGE + 10)
		bitset2.set(BITS_PER_PAGE + 20)

		bitset1.calculate(bitset2, target, 'intersection')
		expect(target.get(BITS_PER_PAGE + 10)).toBe(1)
		expect(target.get(BITS_PER_PAGE + 20)).toBe(0)
	})

	it('should calculate difference correctly across multiple pages', () => {
		const bitset1 = new Bitset()
		const bitset2 = new Bitset()
		const target = new Bitset()

		bitset1.set(BITS_PER_PAGE + 10)
		bitset1.set(BITS_PER_PAGE + 20)
		bitset2.set(BITS_PER_PAGE + 20)

		bitset1.calculate(bitset2, target, 'difference')
		expect(target.get(BITS_PER_PAGE + 10)).toBe(1)
		expect(target.get(BITS_PER_PAGE + 20)).toBe(0)
	})
})
