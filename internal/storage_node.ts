import fs from 'node:fs'
import fsp from 'node:fs/promises'
import path from 'node:path'
import { BLOCK_EXTENSION, type Storage } from './storage.js'
import { throttle } from './utils.js'

export async function createNodeStorage(
	dirPath: string,
	syncDelay: number,
): Promise<Storage> {
	await ensureDir(dirPath)
	const fds = new Map<string, number>()

	const createHandle = (name: string) => {
		const fd = fs.openSync(path.join(dirPath, name), 'a+')
		fds.set(name, fd)
		return fd
	}

	const storage: Storage = {
		appendToBlock: async (name: string, entry: string) => {
			let fd = fds.get(name)
			if (!fd) {
				fd = createHandle(name)
			}

			const buffer = Buffer.from(`${entry}\n`)
			await appendToFile(fd, buffer, syncDelay)
		},

		close: async () => {
			await Promise.all(
				Array.from(fds.values()).map((fd) => {
					fs.closeSync(fd)
				}),
			)
		},

		closeBlock: async (name: string) => {
			const fd = fds.get(name)
			if (fd) {
				fs.closeSync(fd)
				fds.delete(name)
			}
		},

		createBlock: async (pid: string) => {
			await createHandle(
				pid.endsWith(BLOCK_EXTENSION) ? pid : `${pid}${BLOCK_EXTENSION}`,
			)
		},

		deleteBlock: async (name) => {
			await storage.closeBlock(name)
			await fsp.unlink(path.join(dirPath, name))
		},

		getBlocksStats: async () => {
			const files = await getFilesWithExtension(dirPath, BLOCK_EXTENSION)
			return Promise.all(files.map(storage.getBlockStats))
		},

		getBlockStats: async (name) => {
			const stats = await fsp.stat(path.join(dirPath, name))
			return { bid: name, size: stats.size }
		},

		readBlock: readLines.bind(null, dirPath),

		renameBlock: async (oldName, newName) => {
			await fsp.rename(path.join(dirPath, oldName), path.join(dirPath, newName))
		},
	}

	Object.freeze(storage)
	return storage
}

/**
 * Read a file line by line and process each line with a function.
 * @param fileName The path to the file to read
 * @param breakChar character to break the line (default '\n')
 */
export async function* readLines(
	dirPath: string,
	fileName: string,
	breakChar = '\n',
	bufferSize = 1024 * 1024, // 1MB
): AsyncIterableIterator<[string, number]> {
	const fileHandle = await fsp.open(path.join(dirPath, fileName), 'r')
	const rs = fileHandle.createReadStream({
		autoClose: false,
		encoding: 'utf8', // check will be string
		highWaterMark: bufferSize,
	})

	try {
		let lineNo = 0
		let buffer = ''
		for await (const chunk of rs) {
			buffer += chunk
			let lineBreakIndex: number
			// biome-ignore lint/suspicious/noAssignInExpressions: assignment in while condition is intentional
			while ((lineBreakIndex = buffer.indexOf(breakChar)) >= 0) {
				const line = buffer.slice(0, lineBreakIndex)
				if (line.length === 0) {
					throw new Error(
						`Empty line in file:${path.join(dirPath, fileName)} at line:${lineNo}`,
					)
				}

				lineNo++
				yield [line, lineNo]
				buffer = buffer.slice(lineBreakIndex + breakChar.length)
			}
		}
	} finally {
		rs.close()
	}
}

// write a value to the end of the page
export async function appendToFile(
	fd: number,
	buffer: Buffer,
	sync: number,
): Promise<number> {
	if (sync === 0) {
		fs.writeSync(fd, buffer, 0, buffer.byteLength)
		return buffer.byteLength
	}

	return await new Promise<number>((resolve, reject) => {
		fs.write(fd, buffer, 0, buffer.byteLength, null, (err) => {
			if (err) {
				reject(err)
			} else {
				throttle(() => {
					try {
						fs.fdatasyncSync(fd)
					} catch (error) {
						// ignore the error if the file is already closed
						if ((error as NodeJS.ErrnoException).code !== 'EBADF') {
							throw error
						}
					}
				}, sync)()
				resolve(buffer.byteLength)
			}
		})
	})
}

/**
 * Ensure that a directory exists, creating it if necessary
 * @param dirPath The path to the directory to create
 */
export async function ensureDir(dirPath: string) {
	try {
		const s = await fsp.stat(dirPath)
		if (!s.isDirectory()) {
			throw new Error(`Path is not a directory: ${dirPath}`)
		}
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			await fsp.mkdir(dirPath, { recursive: true })
		} else {
			throw err
		}
	}
}

/**
 * Ensure that a file exists, creating it if necessary
 * @param filePath The path to the file to create
 */
export async function ensureFile(filePath: string) {
	try {
		const s = await fsp.stat(filePath)
		if (!s.isFile()) {
			throw new Error(`Path is not a file: ${filePath}`)
		}
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			await fsp.writeFile(filePath, '')
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
	const files = await fsp.readdir(dirPath)
	return files.filter((file) => file.endsWith(extension))
}
