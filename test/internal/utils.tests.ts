import { describe, expect, it } from 'vitest'
import { generateId } from '../../internal/utils'

describe('Utils tests', () => {
	it('should generate unique ids', async () => {
		const ids = new Set()
		for (let i = 0; i < 1000; i++) {
			ids.add(generateId())
		}
		expect(ids.size).toBe(1000)
	})
})
