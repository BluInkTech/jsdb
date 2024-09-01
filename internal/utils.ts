let startId = Date.now()

/**
 * Generate a unique id
 * Note: This is not a cryptographically secure id and should generate
 * duplicates if called more than 1000 times per millisecond. This is
 * used for generating unique ids for pages so that should not be an
 * issue.
 * @returns a unique id
 */
export function generateId() {
	startId++
	return startId.toString(36)
}

/**
 * Debounces a function by delaying its execution until a certain amount of
 * time has passed without any further function calls.
 * @param func The function to debounce
 * @param debounceMs The number of seconds to debounce the function
 * @returns The debounced function
 */

// biome-ignore lint/complexity/noBannedTypes: should work with any function
export function debounce(func: Function, debounceMs: number) {
	let timeoutId: NodeJS.Timeout | null = null

	// biome-ignore lint/suspicious/noExplicitAny: should work with any function
	return function (...args: any[]) {
		clearTimeout(timeoutId as NodeJS.Timeout)
		timeoutId = setTimeout(() => {
			// @ts-ignore
			func.apply(this, args)
		}, debounceMs)
	}
}
