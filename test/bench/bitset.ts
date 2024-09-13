import { RoaringBitmap32 } from 'roaring-wasm/index.cjs'
import { Bitset } from '../../internal/bitset'
import { Benchmark } from '../helpers'

function generateRandomArray(size: number, min: number, max: number): number[] {
	const array: number[] = []
	for (let i = 0; i < size; i++) {
		const randomNum = Math.floor(Math.random() * (max - min + 1)) + min
		array.push(randomNum)
	}
	return array
}

function bitsetPerformance() {
	// create an array with random 20000 number ranging from 1000 to 500000
	const randomArray = generateRandomArray(20000, 1000, 500000)
	const bufferBitset = new Bitset()
	// biome-ignore lint/complexity/noForEach: <explanation>
	randomArray.forEach((x) => bufferBitset.set(x))
	const otherBitset = new Bitset()
	const bitsToSet = generateRandomArray(2000, 1000, 500000)
	for (const bit of bitsToSet) {
		otherBitset.set(bit)
	}

	// target bitset for intersection
	const target = new Bitset()

	Benchmark.add('ArrayBuffer Bitset', () => {
		bufferBitset.calculate(otherBitset, target, 'intersection')
	})

	const roaringBitmap = new RoaringBitmap32()
	roaringBitmap.addMany(randomArray)
	const otherRoaringBitmap = new RoaringBitmap32()
	otherRoaringBitmap.addMany(bitsToSet)

	Benchmark.add('Roaring Bitmap', () => {
		// This is not a like for like test as RoaringBitmap does not have a
		// method for intersection and update a separate target bitmap. There
		// is a cost of creating a new bitmap for each operation.
		roaringBitmap.andNotInPlace(otherRoaringBitmap)
	})

	Benchmark.run(
		'Bitset difference performance 500_000 elements (2000 intersection)',
		1000,
	)
}

/**
13/09/2024 (x4 faster from the naive implementation)
Bitset difference performance 500_000 elements (2000 intersection) (x1,000)
---------------------------------------------------------------------------
✔  Roaring Bitmap.....................................38,964 ops/s..........25.66 μs/op................0.00 % slower
✔  ArrayBuffer Bitset..................................8,223 ops/s.........121.60 μs/op...............78.90 % slower
 */
bitsetPerformance()
