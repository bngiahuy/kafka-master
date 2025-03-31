import fs from 'fs';
import path from 'path';

// Thêm hàm để ghi log vào file nhất định
export const logToFile = (filePath, message) => {
	const content = `[${new Date().toISOString()}] ${message}\n`;

	try {
		// Đảm bảo thư mục tồn tại
		const dir = path.dirname(filePath);
		if (!fs.existsSync(dir)) {
			fs.mkdirSync(dir, { recursive: true });
		}

		fs.appendFileSync(filePath, content);
	} catch (err) {
		console.error(`Lỗi khi ghi log vào file ${filePath}:`, err);
	}
};

// Export các hàm log chuyên biệt
export const logBatchSuccess = (batchInfo) => {
	logToFile('./logs/finish-batches.log', JSON.stringify(batchInfo));
};

export const logBatchFailure = (batchInfo, reason) => {
	logToFile('./logs/fail-batches.log', `${JSON.stringify(batchInfo)} - Reason: ${reason}`);
};

export const logMessage = (message) => {
	logToFile('./logs/general-logs.log', message);
};

