export const PAGE_SIZE = 256 // Number of 32-bit integers per page (256 * 4 bytes = 1024 bytes)
export const BITS_PER_PAGE = PAGE_SIZE * 32 // Number of bits per page

/**
 * Represents a sparse Bitset data structure using paging.
 */
export class Bitset {
	private pages: Map<number, Int32Array>

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
	 * Resizes the bitset to the specified size.
	 *
	 * @param size - The desired size of the bitset.
	 */
	resize(size: number) {
		const numPages = Math.ceil(size / BITS_PER_PAGE)
		for (let i = 0; i < numPages; i++) {
			if (!this.pages.has(i)) {
				this.pages.set(i, new Int32Array(PAGE_SIZE))
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
		const intIndex = Math.floor(bitIndex / 32)
		const bitPosition = bitIndex % 32

		let page = this.pages.get(pageIndex)
		if (!page) {
			page = new Int32Array(PAGE_SIZE)
			this.pages.set(pageIndex, page)
		}
		;(page[intIndex] as number) |= 1 << bitPosition
	}

	/**
	 * Clears the bit at the specified index.
	 *
	 * @param bit - The index of the bit to clear.
	 */
	clear(bit: number) {
		const pageIndex = Math.floor(bit / BITS_PER_PAGE)
		const bitIndex = bit % BITS_PER_PAGE
		const intIndex = Math.floor(bitIndex / 32)
		const bitPosition = bitIndex % 32

		const page = this.pages.get(pageIndex)
		if (!page) {
			return
		}
		;(page[intIndex] as number) &= ~(1 << bitPosition)

		// Optionally remove the page if it becomes empty
		if (page.every((int) => int === 0)) {
			this.pages.delete(pageIndex)
		}
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
		const intIndex = Math.floor(bitIndex / 32)
		const bitPosition = bitIndex % 32

		const page = this.pages.get(pageIndex)
		if (!page) {
			return 0
		}
		return ((page[intIndex] as number) & (1 << bitPosition)) !== 0 ? 1 : 0
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
			const thisPage = this.pages.get(pageIndex) || new Int32Array(PAGE_SIZE)
			const otherPage = other.pages.get(pageIndex) || new Int32Array(PAGE_SIZE)
			let targetPage = target.pages.get(pageIndex)

			if (!targetPage) {
				targetPage = new Int32Array(PAGE_SIZE)
				target.pages.set(pageIndex, targetPage)
			}

			for (let i = 0; i < PAGE_SIZE; i++) {
				if (operation === 'difference') {
					targetPage[i] = (thisPage[i] as number) & ~(otherPage[i] as number)
				} else if (operation === 'intersection') {
					targetPage[i] = (thisPage[i] as number) & (otherPage[i] as number)
				} else if (operation === 'union') {
					targetPage[i] = (thisPage[i] as number) | (otherPage[i] as number)
				}
			}

			// Optionally remove the target page if it becomes empty
			if (targetPage.every((int) => int === 0)) {
				target.pages.delete(pageIndex)
			}
		}
	}

	/**
	 * Sets multiple bits at once.
	 *
	 * @param bits - An array of bit indices to set.
	 */
	setMultiple(bits: number[]) {
		for (const bit of bits) {
			this.set(bit)
		}
	}

	/**
	 * Clears multiple bits at once.
	 *
	 * @param bits - An array of bit indices to clear.
	 */
	clearMultiple(bits: number[]) {
		for (const bit of bits) {
			this.clear(bit)
		}
	}

	/**
	 * Gets the values of multiple bits at once.
	 *
	 * @param bits - An array of bit indices to get.
	 * @returns An array of bit values (0 or 1).
	 */
	getMultiple(bits: number[]): number[] {
		return bits.map((bit) => this.get(bit))
	}

	/**
	 * Returns an iterator that iterates over the set bits in the bitset.
	 */
	*[Symbol.iterator]() {
		for (const [pageIndex, page] of this.pages) {
			for (let intIndex = 0; intIndex < PAGE_SIZE; intIndex++) {
				const int = page[intIndex]
				if (int === 0) continue

				for (let bitPosition = 0; bitPosition < 32; bitPosition++) {
					if ((int as number) & (1 << bitPosition)) {
						yield pageIndex * BITS_PER_PAGE + intIndex * 32 + bitPosition
					}
				}
			}
		}
	}
}
