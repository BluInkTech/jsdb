import { mkdir, readdir, stat } from 'node:fs/promises'
import path from 'node:path'
/**
 * Ensure that a directory exists, creating it if necessary
 * @param dirPath The path to the directory to create
 */
export async function ensureDir(dirPath: string) {
	try {
		const s = await stat(dirPath)
		if (!s.isDirectory()) {
			throw new Error(`Path is not a directory: ${dirPath}`)
		}
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			await mkdir(dirPath, { recursive: true })
		} else {
			throw err
		}
	}
}

/**
 * Get a list of files in a directory with a specific extension
 * @param dirPath The path to the directory to search
 * @param extension The file extension to search for
 * @returns A list of files in the directory with the specified extension
 */
export async function getFilesWithExtension(
	dirPath: string,
	extension: string,
) {
	const files = await readdir(dirPath)
	return files
		.filter((file) => file.endsWith(extension))
		.map((file) => path.join(dirPath, file))
}
