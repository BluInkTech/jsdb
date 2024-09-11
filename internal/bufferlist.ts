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

/**
 * Compares a compound key with an array of values.
 *
 * @param array - The array containing the compound key.
 * @param offset - The offset in the array where the key starts.
 * @param id - The array of values representing the key to compare.
 * @param keyLength - The length of the key.
 * @returns A number indicating the comparison result:
 *          -1 if the compound key is less than the given key,
 *           0 if they are equal,
 *           1 if the compound key is greater than the given key.
 * @throws Error if the length of the given key is invalid.
 */
export function compareCompoundKey(
	array: Uint32Array,
	offset: number,
	id: number[],
	keyLength: number,
): number {
	if (id.length < keyLength) {
		throw new Error('Invalid key length')
	}

	for (let i = 0; i < keyLength; i++) {
		if ((array[offset + i] as number) < (id[i] as number)) {
			return -1
		}
		if ((array[offset + i] as number) > (id[i] as number)) {
			return 1
		}
	}
	return 0
}

/**
 * Performs a binary search on a sorted array of compound keys to find the
 * leftmost occurrence of a given key.
 *
 * @param array - The sorted array of compound keys.
 * @param id - The key to search for.
 * @param stride - The stride of each compound key in the array.
 * @param keyLength - The length of the key.
 * @returns A tuple containing a boolean indicating if the key was found and the
 * index of the leftmost occurrence of the key.
 */
function bisectLeftCompound(
	array: Uint32Array,
	id: number[],
	stride: number,
	keyLength: number,
): [boolean, number] {
	let low = 0
	let high = array.length / stride

	while (low < high) {
		const mid = (low + high) >> 1
		const cmp = compareCompoundKey(array, mid * stride, id, keyLength)
		if (cmp === 0) {
			return [true, mid * stride]
		}
		if (cmp < 0) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return [false, low * stride]
}

/**
 * Represents a buffer list that stores a sequence of Uint32 numbers in an
 * underlying ArrayBuffer.
 * The buffer list allows for efficient insertion, deletion, and retrieval of
 * numbers.
 */
export class BufferList {
	public buffer: ArrayBuffer
	public array: Uint32Array
	private arrayLen = 0
	private stride: number

	constructor(
		public keyLen: number,
		public valueLen: number,
		parameters?: {
			init?: number[]
			size?: number
		},
	) {
		this.stride = keyLen + valueLen
		if (
			parameters?.size &&
			parameters?.size < this.stride * Int32Array.BYTES_PER_ELEMENT
		) {
			throw new Error('Size is too small for the given stride')
		}

		let initialSize = 4096
		if (parameters?.size) {
			initialSize = parameters.size
		}

		if (
			parameters?.init &&
			parameters.init.length * Int32Array.BYTES_PER_ELEMENT > initialSize
		) {
			initialSize = parameters.init.length * Int32Array.BYTES_PER_ELEMENT
		}

		this.buffer = new ArrayBuffer(initialSize, {
			maxByteLength: SAFE_ARRAY_SIZE,
		})

		this.array = new Uint32Array(this.buffer)
		if (parameters?.init) {
			this.array.set(parameters.init)
			this.arrayLen = parameters.init.length
		}
	}

	/**
	 * Gets the length of the buffer list.
	 *
	 * @returns The length of the buffer list.
	 */
	get length() {
		return this.arrayLen
	}

	/**
	 * Gets the count of items in the buffer list.
	 *
	 * @returns The count of items.
	 */
	get itemsCount() {
		return this.length / this.stride
	}

	/**
	 * Expands the buffer.
	 * The buffer size is doubled and resized to a multiple of 4096, ensuring it
	 * is greater than the new length. If the resulting size exceeds the maximum
	 * limit, an error is thrown.
	 */
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

	/**
	 * Retrieves the element at the specified index.
	 *
	 * @param index - The index of the element to retrieve.
	 * @returns The element at the specified index, or undefined if the index
	 * is out of range.
	 */
	get(index: number): number | undefined {
		// We want to only allow a user to access the elements
		// in the restricted range and not the entire underlying
		// array buffer.
		if (index < 0 || index >= this.length) {
			return undefined
		}
		return this.array[index]
	}

	/**
	 * Sets the value at the specified index in the buffer list.
	 *
	 * @param index - The index at which to set the value.
	 * @param value - The value to set.
	 * @throws Error if the index is out of bounds.
	 */
	set(index: number, value: number) {
		if (index < 0 || index >= this.length) {
			throw new Error('Index out of bounds')
		}
		this.array[index] = value
	}

	/**
	 * Sets a sorted record in the buffer list.
	 *
	 * @param record - The record to be set.
	 * @throws {Error} If the length of the record is invalid.
	 */
	setSorted(record: number[]) {
		if (record.length !== this.stride) {
			throw new Error('Invalid record length')
		}

		const [exists, index] = bisectLeftCompound(
			this.array.subarray(0, this.length),
			record,
			this.stride,
			this.keyLen,
		)
		if (exists) {
			this.array.set(record, index)
		} else {
			this.expand()
			if (this.length !== 0) {
				// we need to shift the rest of the array to the right
				// and adjust the length as we are adding a new value
				this.array.copyWithin(index + this.stride, index, this.arrayLen)
			}

			this.array.set(record, index)
			this.arrayLen += record.length
		}
	}

	/**
	 * Retrieves a sorted record from the buffer list.
	 *
	 * @param record - The record to search for.
	 * @returns The sorted record if found, otherwise undefined.
	 */
	getSorted(record: number[]): Uint32Array | undefined {
		if (this.length === 0) {
			return undefined
		}
		const [exists, index] = bisectLeftCompound(
			this.array.subarray(0, this.length),
			record,
			this.stride,
			this.keyLen,
		)

		if (!exists) {
			return undefined
		}
		return this.array.subarray(index, index + this.stride)
	}

	/**
	 * Removes a sorted record from the buffer list.
	 *
	 * @param record - The record to be removed.
	 * @returns The removed record, or undefined if it doesn't exist.
	 */
	removeSorted(record: number[]) {
		const [exists, index] = bisectLeftCompound(
			this.array.subarray(0, this.length),
			record,
			this.stride,
			this.keyLen,
		)

		if (!exists) {
			return undefined
		}

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

	/**
	 * Adds a value to the buffer list.
	 *
	 * @param value - The value to be added.
	 */
	push(value: number) {
		this.expand()
		this.array[this.arrayLen++] = value
	}

	/**
	 * Removes and returns the last element from the buffer list.
	 * If the buffer list is empty, returns undefined.
	 *
	 * @returns The last element from the buffer list, or undefined if the
	 * buffer list is empty.
	 */
	pop(): number | undefined {
		if (this.arrayLen === 0) {
			return undefined
		}
		return this.array[--this.arrayLen]
	}
}
