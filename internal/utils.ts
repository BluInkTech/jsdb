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
