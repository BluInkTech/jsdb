export const PAGE_SIZE = 512 // Number of bytes per page
export const BITS_PER_PAGE = PAGE_SIZE * 8 // Number of bits per page

/**
 * Represents a sparse Bitset data structure using paging.
 */
export class Bitset {
	private pages: Map<number, Uint8Array>

	constructor() {
		this.pages = new Map()
	}

	/**
	 * Gets the length of the bitset.
	 *
	 * @returns The length of the bitset.
	 */
	get length() {
		return this.pages.size * BITS_PER_PAGE
	}

	/**
	 * Resizes the bitset to the nearest specified size which will be a multiple
	 * of PAGE_SIZE.
	 *
	 * @param size - The desired size of the bitset.
	 */
	resize(size: number) {
		const numPages = Math.ceil(size / BITS_PER_PAGE)
		for (let i = 0; i < numPages; i++) {
			if (!this.pages.has(i)) {
				this.pages.set(i, new Uint8Array(PAGE_SIZE))
			}
		}
	}

	/**
	 * Sets the bit at the specified index.
	 *
	 * @param bit - The index of the bit to set.
	 */
	set(bit: number) {
		const pageIndex = Math.floor(bit / BITS_PER_PAGE)
		const bitIndex = bit % BITS_PER_PAGE
		const byteIndex = Math.floor(bitIndex / 8)
		const bitPosition = bitIndex % 8

		if (!this.pages.has(pageIndex)) {
			this.pages.set(pageIndex, new Uint8Array(PAGE_SIZE))
		}

		const page = this.pages.get(pageIndex)
		page[byteIndex] |= 1 << bitPosition
	}

	/**
	 * Clears the bit at the specified index.
	 *
	 * @param bit - The index of the bit to clear.
	 */
	clear(bit: number) {
		const pageIndex = Math.floor(bit / BITS_PER_PAGE)
		const bitIndex = bit % BITS_PER_PAGE
		const byteIndex = Math.floor(bitIndex / 8)
		const bitPosition = bitIndex % 8

		if (!this.pages.has(pageIndex)) {
			return
		}

		const page = this.pages.get(pageIndex)
		page[byteIndex] &= ~(1 << bitPosition)
	}

	/**
	 * Gets the bit at the specified index.
	 *
	 * @param bit - The index of the bit to get.
	 * @returns The value of the bit (0 or 1).
	 */
	get(bit: number): number {
		const pageIndex = Math.floor(bit / BITS_PER_PAGE)
		const bitIndex = bit % BITS_PER_PAGE
		const byteIndex = Math.floor(bitIndex / 8)
		const bitPosition = bitIndex % 8

		if (!this.pages.has(pageIndex)) {
			return 0
		}

		const page = this.pages.get(pageIndex)
		return (page[byteIndex] & (1 << bitPosition)) !== 0 ? 1 : 0
	}

	/**
	 * Clears all bits in the bitset.
	 */
	clearAll() {
		this.pages.clear()
	}

	/**
	 * Calculates the union, intersection, or difference of two bitsets.
	 *
	 * @param other - The other bitset to calculate the operation with.
	 * @param target - The target bitset to store the result.
	 * @param operation - The operation ('difference', 'intersection', 'union').
	 */
	calculate(
		other: Bitset,
		target: Bitset,
		operation: 'difference' | 'intersection' | 'union',
	) {
		const thisPages = Array.from(this.pages.keys())
		const otherPages = Array.from(other.pages.keys())
		const allPages = new Set([...thisPages, ...otherPages])

		for (const pageIndex of allPages) {
			const thisPage = this.pages.get(pageIndex) || new Uint8Array(PAGE_SIZE)
			const otherPage = other.pages.get(pageIndex) || new Uint8Array(PAGE_SIZE)
			let targetPage = target.pages.get(pageIndex)

			if (!targetPage) {
				targetPage = new Uint8Array(PAGE_SIZE)
				target.pages.set(pageIndex, targetPage)
			}

			for (let i = 0; i < PAGE_SIZE; i++) {
				if (operation === 'difference') {
					targetPage[i] = thisPage[i] & ~otherPage[i]
				} else if (operation === 'intersection') {
					targetPage[i] = thisPage[i] & otherPage[i]
				} else if (operation === 'union') {
					targetPage[i] = thisPage[i] | otherPage[i]
				}
			}
		}
	}

	/**
	 * Returns an iterator that iterates over the set bits in the bitset.
	 */
	*[Symbol.iterator]() {
		for (const [pageIndex, page] of this.pages) {
			for (let byteIndex = 0; byteIndex < PAGE_SIZE; byteIndex++) {
				const byte = page[byteIndex]
				if (byte === 0) continue

				for (let bitPosition = 0; bitPosition < 8; bitPosition++) {
					if (byte & (1 << bitPosition)) {
						yield pageIndex * BITS_PER_PAGE + byteIndex * 8 + bitPosition
					}
				}
			}
		}
	}
}
