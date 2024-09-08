import v8 from 'node:v8'
// import avro from 'avro-js'
import { bench, describe } from 'vitest'

const testObj = {
	id: '1',
	_seq: 100,
	foo: 'bar',
	name: 'John Doe',
	age: 30,
	address: '123 Main St',
	city: 'Anytown',
	state: 'AS',
	zip: '12345',
}
// const schema = avro.parse({
// 	name: 'Person',
// 	type: 'record',
// 	fields: [
// 		{ name: 'id', type: 'string' },
// 		{ name: '_seq', type: 'int' },
// 		{ name: 'foo', type: 'string' },
// 		{ name: 'name', type: 'string' },
// 		{ name: 'age', type: 'int' },
// 		{ name: 'address', type: 'string' },
// 		{ name: 'city', type: 'string' },
// 		{ name: 'state', type: 'string' },
// 		{ name: 'zip', type: 'string' },
// 	],
// })

console.log('Json size:', JSON.stringify(testObj).length)
console.log('v8 size:', v8.serialize(testObj).byteLength)
// console.log('avro size:', schema.toBuffer(testObj).byteLength)

describe('Serialisation', () => {
	bench('JSON.stringify', () => {
		JSON.stringify(testObj)
	})

	bench('v8 serialize', () => {
		v8.serialize(testObj)
	})

	// bench('avro.encode', () => {
	// 	schema.toBuffer(testObj)
	// })
})

describe('DeSerialisation', () => {
	const test = JSON.stringify(testObj)
	const testBuffer = v8.serialize(testObj)
	// const testAvro = schema.toBuffer(testObj)
	bench('JSON.parse', () => {
		JSON.parse(test)
	})

	bench('v8 deserialize', () => {
		v8.deserialize(testBuffer)
	})

	// bench('avro.decode', () => {
	// 	schema.fromBuffer(testAvro)
	// })
})
