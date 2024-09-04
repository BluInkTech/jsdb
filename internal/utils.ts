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

/**
 * Throttles a function by limiting the rate at which it can be called.
 * @param func The function to throttle
 * @param throttleMs The number of seconds to throttle the function
 * @returns The throttled function
 */
// biome-ignore lint/complexity/noBannedTypes: should work with any function
export function throttle(func: Function, throttleMs: number) {
	let lastCall = 0

	// biome-ignore lint/suspicious/noExplicitAny: should work with any function
	return function (...args: any[]) {
		const now = Date.now()
		if (now - lastCall >= throttleMs) {
			lastCall = now
			// @ts-ignore
			func.apply(this, args)
		}
	}
}
