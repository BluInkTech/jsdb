import type { JsDbOptions } from '../index.js'
import type { PageGroup } from './pagegroup.js'

/**
 * Database state to be shared around.
 * @internal
 */
export type DbState = {
	data: PageGroup
	logs: PageGroup
	seqNo: number
	timers: NodeJS.Timeout[]
	opts: JsDbOptions
	opened: boolean
}

/**
 * Create a JsDbOptions object with the given options. If the options are not
 * valid, an error will be thrown.
 * @param options options to create a JsDbOptions object
 * @returns a JsDbOptions object
 */
export function createOptions(options: Partial<JsDbOptions>): JsDbOptions {
	const opts: JsDbOptions = {
		dirPath: '',
		maxPageSize: 1024 * 1024 * 8, // 8 MB
		dataSyncDelay: 1000,
		staleDataThreshold: 0.1,
		comapctDelay: 1000 * 60 * 60 * 24, // 24 hours
		cachedFields: [],
	}

	if (!options.dirPath) {
		throw new Error('dirPath is required')
	}
	opts.dirPath = options.dirPath

	if (options.maxPageSize) {
		if (options.maxPageSize < 1024 || options.maxPageSize % 1024 !== 0) {
			throw new Error('maxPageSize must be at least 1024 KB')
		}
		opts.maxPageSize = options.maxPageSize
	}

	if (options.dataSyncDelay) {
		opts.dataSyncDelay = options.dataSyncDelay
	}

	if (options.staleDataThreshold) {
		if (options.staleDataThreshold < 0 || options.staleDataThreshold > 1) {
			throw new Error('staleDataThreshold must be between 0 and 1')
		}
		opts.staleDataThreshold = options.staleDataThreshold
	}

	if (options.comapctDelay) {
		opts.comapctDelay = options.comapctDelay
	}

	if (
		options.cachedFields &&
		Array.isArray(options.cachedFields) &&
		options.cachedFields.length > 0
	) {
		opts.cachedFields = options.cachedFields
	}
	return opts
}
