import type { Id } from '../index.js'
const SAFE_ARRAY_SIZE = 1 << 30

// the following are missing from the lib.d.ts
declare global {
	interface ArrayBuffer {
		/**
		 * Resize the buffer to the new size
		 * @param size The new size of the buffer
		 */
		resize(size: number): void
	}

	interface ArrayBufferConstructor {
		/**
		 * Create a new ArrayBuffer with the given size and options
		 * @param size The size of the buffer
		 * @param options The options for the buffer
		 */

		new (size: number, options: { maxByteLength: number }): ArrayBuffer
	}
}

/**
 * Find the index of the first item in the array that is greater than or
 * equal to the id.
 *
 * The custom comparator function is used to compare the id with the
 * items in the array. The comparator function should return
 * - a negative number if the id is less than the item
 * - a positive number if the id is greater than the item
 * - zero if the id is equal to the item
 *
 * @param arr The array to search
 * @param id The id to search for
 * @param stride The width of the key-value pair (1 for single values)
 * @param comparator The comparator function to use
 * @returns The index of the item
 */
export function bisectLeft<T>(
	arr: ArrayLike<T>,
	id: Id,
	stride: number,
	comparator: (a: T, b: Id) => number = (a, b) => (a < b ? -1 : a > b ? 1 : 0),
) {
	let start = 0
	let end = arr.length - stride

	while (start <= end) {
		let mid = (start + end) >> 1
		if (stride === 2) {
			// Ensure mid is even otherwise subtract 1
			if (mid & 1) {
				mid--
			}
		} else if (stride > 2) {
			mid -= mid % stride
		}

		const cmp = comparator(arr[mid] as T, id)

		if (cmp === 0) {
			return mid
		}

		if (cmp < 0) {
			start = mid + stride
		} else {
			end = mid - stride
		}
	}

	return start
}

export function setValue(
	segment: Uint32Array,
	index: number,
	values: number[],
) {
	// Insert the new value at the specified index
	// use loop unrolling for performance
	if (values.length === 1) {
		segment[index + 1] = values[0] as number
	} else if (values.length === 2) {
		segment[index + 1] = values[0] as number
		segment[index + 2] = values[1] as number
	} else {
		// add the rest of the values
		for (let i = 0; i < values.length; i++) {
			segment[index + i + 1] = values[i] as number
		}
	}
}

export class BufferList {
	public buffer: ArrayBuffer
	public array: Uint32Array
	private arrayLen = 0
	private stride: number

	constructor(parameters?: {
		stride?: number
		init?: number[]
		size?: number
	}) {
		this.stride = parameters?.stride || 1
		if (
			parameters?.size &&
			parameters?.size < this.stride * Int32Array.BYTES_PER_ELEMENT
		) {
			throw new Error('Size is too small for the given stride')
		}

		this.buffer = new ArrayBuffer(parameters?.size || 4096, {
			maxByteLength: SAFE_ARRAY_SIZE,
		})

		this.array = new Uint32Array(this.buffer)
		if (parameters?.init) {
			this.array.set(parameters.init)
			this.arrayLen = parameters.init.length
		}
	}

	get length() {
		return this.arrayLen
	}

	get itemsCount() {
		return this.length / this.stride
	}

	expand() {
		if (this.arrayLen >= this.array.length) {
			// find the ideal size for the buffer which should be a multiple of 4096
			// and greater than the new length
			// double the size of the buffer
			const size = Math.ceil((this.array.length * 2) / 4096) * 4096
			if (size > SAFE_ARRAY_SIZE) {
				throw new Error('Array size exceeds the maximum limit')
			}
			this.buffer.resize(size)
			this.array = new Uint32Array(this.buffer)
		}
	}

	copyWithin(target: number, start: number) {
		// there is no point in copying beyond the computed length
		this.array.copyWithin(target, start, this.arrayLen)
	}

	get(index: number): number | undefined {
		// We want to only allow a user to access the elements
		// in the restricted range and not the entire underlying
		// array buffer.
		if (index < 0 || index >= this.length) {
			return undefined
		}
		return this.array[index]
	}

	set(index: number, value: number) {
		if (index < 0 || index >= this.length) {
			throw new Error('Index out of bounds')
		}
		this.array[index] = value
	}

	setSorted(id: number, values: number[]) {
		if (values.length + 1 !== this.stride) {
			throw new Error('Invalid value length')
		}

		const index = bisectLeft(
			this.array.subarray(0, this.length),
			id,
			this.stride,
		)
		if (this.array[index] === id) {
			setValue(this.array, index, values)
		} else {
			this.expand()
			if (this.length !== 0) {
				// we need to shift the rest of the array to the right
				// and adjust the length as we are adding a new value
				this.array.copyWithin(index + this.stride, index, this.arrayLen)
			}
			this.array[index] = id
			setValue(this.array, index, values)
			this.arrayLen += this.stride
		}
	}

	getSorted(id: number): Uint32Array | undefined {
		if (this.length === 0) {
			return undefined
		}
		const index = bisectLeft(
			this.array.subarray(0, this.length),
			id,
			this.stride,
		)
		if (this.array[index] !== id) {
			return undefined
		}
		return this.array.subarray(index, index + this.stride)
	}

	removeSorted(id: number) {
		const index = bisectLeft(
			this.array.subarray(0, this.arrayLen),
			id,
			this.stride,
		)
		if (this.array[index] !== id) return

		// removing means we need to shift the rest of the array towards the left
		// copywithin is faster than set operation on the same array
		if (index + this.stride !== this.arrayLen) {
			// it will be a no op as there is nothing to copy
			this.array.copyWithin(index, index + this.stride, this.arrayLen)
		} else {
			// we should manually set the values to 0
			for (let i = 0; i < this.stride; i++) {
				this.array[index + i] = 0
			}
		}
		this.arrayLen -= this.stride
	}

	push(value: number) {
		this.expand()
		this.array[this.arrayLen++] = value
	}

	pop(): number | undefined {
		if (this.arrayLen === 0) {
			return undefined
		}
		return this.array[--this.arrayLen]
	}
}
