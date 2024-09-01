import { describe, expect, it } from 'vitest'
import { debounce, generateId } from '../internal/utils'

describe('Utils tests', () => {
	it('should generate unique ids', async () => {
		const ids = new Set()
		for (let i = 0; i < 1000; i++) {
			ids.add(generateId())
		}
		expect(ids.size).toBe(1000)
	})

	it('should debounce function calls', async () => {
		let count = 0
		const debounced = debounce(() => {
			count++
		}, 100)

		debounced()
		debounced()
		debounced()
		debounced()
		debounced()
		debounced()

		await new Promise((resolve) => setTimeout(resolve, 200))
		expect(count).toBe(1)
	})
})
