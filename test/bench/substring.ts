import { bench, describe, it } from 'vitest'

const testString = 'hello\x1Fworld\x1Fhow\x1Fare\x1Fyou'
const testBuffer = Buffer.from(testString)
const bufferYou = Buffer.from('you')

// Unit separator is a standard ASCII control character
const unitSepCode = 31
const UnitSep = String.fromCharCode(31)

describe('String vs Buffer delimited parsing', () => {
	bench('String split', () => {
		const result = testString.split(UnitSep)
		if (result[4] !== 'you') {
			throw new Error('Not found')
		}
	})

	bench('Manual split string', () => {
		let start = 0
		const length = testString.length
		const result: string[] = []
		for (let i = 0; i < length; i++) {
			if (testString[i] === UnitSep) {
				result.push(testString.substring(start, i))
				start = i + 1
			}
		}
		result.push(testString.substring(start))
		if (result[4] !== 'you') {
			throw new Error('Not found')
		}
	})

	bench('Buffer', () => {
		let start = 0
		const length = testBuffer.length
		const result: Buffer[] = []
		for (let i = 0; i < length; i++) {
			if (testBuffer[i] === unitSepCode) {
				result.push(testBuffer.subarray(start, i))
				start = i + 1
			}
		}
		result.push(testBuffer.subarray(start))
		if (result[4][0] !== bufferYou[0]) {
			throw new Error('Not found')
		}
	})
})
