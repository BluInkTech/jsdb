import { describe, expect, it } from 'vitest'
import { Bitset } from '../internal/bitset'

describe('Bitset', () => {
	it('should initialize with the correct size', () => {
		const bitset = new Bitset(10)
		expect(bitset.length).toBe(10)
	})

	it('should set and get bits correctly', () => {
		const bitset = new Bitset()
		bitset.set(5)
		expect(bitset.get(5)).toBe(true)
		expect(bitset.get(4)).toBe(false)
	})

	it('should clear bits correctly', () => {
		const bitset = new Bitset(10)
		bitset.set(5)
		expect(bitset.get(5)).toBe(true)
		bitset.clear(5)
		expect(bitset.get(5)).toBe(false)
	})

	it('should clear all bits correctly', () => {
		const bitset = new Bitset(10)
		bitset.set(5)
		bitset.set(6)
		bitset.clearAll()
		expect(bitset.get(5)).toBe(false)
		expect(bitset.get(6)).toBe(false)
	})

	it('should resize correctly', () => {
		const bitset = new Bitset(1)
		bitset.set(15)
		expect(bitset.length).toEqual(1024)
		expect(bitset.get(15)).toBe(true)
	})

	it('should calculate union correctly', () => {
		const bitset1 = new Bitset(10)
		const bitset2 = new Bitset(10)
		const target = new Bitset(10)
		bitset1.set(1)
		bitset2.set(2)
		bitset1.calculate(bitset2, target, 'union')
		expect(target.get(1)).toBe(true)
		expect(target.get(2)).toBe(true)
	})

	it('should calculate intersection correctly', () => {
		const bitset1 = new Bitset(10)
		const bitset2 = new Bitset(10)
		const target = new Bitset(10)
		bitset1.set(1)
		bitset2.set(1)
		bitset1.calculate(bitset2, target, 'intersection')
		expect(target.get(1)).toBe(true)
		expect(target.get(2)).toBe(false)
	})

	it('should calculate difference correctly', () => {
		const bitset1 = new Bitset(10)
		const bitset2 = new Bitset(10)
		const target = new Bitset(10)
		bitset1.set(1)
		bitset1.set(2)
		bitset2.set(2)
		bitset1.calculate(bitset2, target, 'difference')
		expect(target.get(1)).toBe(true)
		expect(target.get(2)).toBe(false)
	})

	it('should iterate over set bits correctly', () => {
		const bitset = new Bitset(10)
		bitset.set(1)
		bitset.set(3)
		const bits = Array.from(bitset)
		expect(bits).toEqual([1, 3])
	})
})
