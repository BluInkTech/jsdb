const SAFE_ARRAY_SIZE = 1 << 30

/**
 * Represents a Bitset data structure.
 */
export class Bitset {
	buffer: ArrayBuffer
	array: Uint8Array

	constructor(size = 1024) {
		this.buffer = new ArrayBuffer(size, {
			maxByteLength: SAFE_ARRAY_SIZE,
		})
		this.array = new Uint8Array(this.buffer)
	}

	/**
	 * Gets the length of the bitset.
	 *
	 * @returns The length of the bitset.
	 */
	get length() {
		return this.array.length
	}

	/**
	 * Resizes the bitset to the specified size.
	 *
	 * @param size - The desired size of the bitset.
	 */
	resize(size: number) {
		// ensure size is a multiple of 1024
		const s = Math.ceil(size / 1024) * 1024
		this.buffer.resize(s)
		this.array = new Uint8Array(this.buffer)
	}

	/**
	 * Sets the bit at the specified index.
	 *
	 * @param bit - The index of the bit to set.
	 */
	set(bit: number) {
		//  find the index of the byte in the array where the bit is located
		const byteIndex = bit >> 3 // Same as Math.floor(bit / 8)

		// find the bit position in the byte
		const bitPosition = bit & 7 // Same as bit % 8
		if (this.array.length <= byteIndex) {
			this.resize(byteIndex)
		}

		if (this.array[byteIndex] === undefined) {
			this.array[byteIndex] = 0
		}
		this.array[byteIndex] |= 1 << bitPosition
	}

	/**
	 * Gets the bit at the specified index.
	 *
	 * @param bit - The index of the bit to get.
	 * @returns The value of the bit at the specified index.
	 */
	get(bit: number) {
		const byteIndex = bit >> 3 // Same as Math.floor(bit / 8)
		const bitPosition = bit & 7 // Same as bit % 8
		if (this.array.length <= byteIndex || this.array[byteIndex] === undefined) {
			return false
		}

		return (this.array[byteIndex] & (1 << bitPosition)) !== 0
	}

	/**
	 * Clears the bit at the specified index.
	 *
	 * @param bit - The index of the bit to clear.
	 */
	clear(bit: number) {
		const byteIndex = bit >> 3 // Same as Math.floor(bit / 8)
		const bitPosition = bit & 7 // Same as bit % 8
		if (this.array.length <= byteIndex || this.array[byteIndex] === undefined) {
			return
		}

		this.array[byteIndex] &= ~(1 << bitPosition)
	}

	/**
	 * Clears all bits in the bitset.
	 */
	clearAll() {
		this.array.fill(0)
	}

	/**
	 * Calculates the union, intersection, or difference of two bitset.
	 *
	 * @param other - The other bitset to calculate the operation with.
	 * @param target - The target bitset to store the result.
	 * @param operation - The operation
	 */
	calculate(
		other: Bitset,
		target: Bitset,
		operation: 'difference' | 'intersection' | 'union',
	) {
		if (this.array.length !== other.array.length) {
			throw new Error('Bitset length mismatch')
		}

		// if target is smaller than the current bitset, resize it
		if (target.array.length < this.array.length) {
			target.resize(this.array.length)
		}

		const length = this.array.length
		for (let i = 0; i < length; i++) {
			if (this.array[i] === undefined || other.array[i] === undefined) {
				continue
			}

			if (operation === 'difference') {
				target.array[i] =
					(this.array[i] as number) & ~(other.array[i] as number)
			} else if (operation === 'intersection') {
				target.array[i] = (this.array[i] as number) & (other.array[i] as number)
			} else if (operation === 'union') {
				target.array[i] = (this.array[i] as number) | (other.array[i] as number)
			}
		}
	}

	/**
	 * Returns an iterator that iterates over the set bits in the bitset.
	 */
	*[Symbol.iterator]() {
		const length = this.array.length
		for (let i = 0; i < length; i++) {
			const byte = this.array[i]
			if (byte === undefined) {
				continue
			}

			for (let j = 0; j < 8; j++) {
				if ((byte & (1 << j)) !== 0) {
					yield i * 8 + j
				}
			}
		}
	}
}
