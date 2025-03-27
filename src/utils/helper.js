import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';

export function getIpFiles(folderPath) {
	try {
		const files = readdirSync(folderPath)
			.filter((file) => file.endsWith('.txt'))
			.map((file) => join(folderPath, file))
			.sort();
		return files;
	} catch (e) {
		console.error(`Error reading directory ${folderPath}: ${e}`);
		return [];
	}
}

export function readIpFile(filePath) {
	try {
		const data = readFileSync(filePath, 'utf8');
		return data.split('\n').filter((line) => line.trim());
	} catch (e) {
		console.error(`Error reading file ${filePath}: ${e}`);
		return [];
	}
}
