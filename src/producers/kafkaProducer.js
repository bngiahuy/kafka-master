import 'dotenv/config';
import kafka from '../configs/kafkaConfig.js';
import fs from 'fs';
import path from 'path';
import redis from '../configs/redisConfig.js';
import { logMessage } from '../utils/logger.js';
import { acquireLock, releaseLock } from '../utils/helper.js';

const producer = kafka.producer();

const DATA_DIR = process.env.CLIENT_DATA_PATH + '/input';
const PROCESSED_FILES_KEY = 'processed:files'; // Key cho Redis Set
const CHECK_INTERVAL = 5000; // Kiểm tra file mới mỗi 5 giây (5000ms)
const WORKER_POLL_INTERVAL = 100;
let isRunning = true; // Biến cờ để dừng vòng lặp chính

// --- Helper function để chờ ---
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Hàm đọc và lọc file mới ---
const getNewFiles = async () => {
	try {
		const allFilesInDir = fs
			.readdirSync(DATA_DIR)
			.filter((f) => f.endsWith('.txt'));
		const processedFiles = await redis.smembers(PROCESSED_FILES_KEY);
		const processedFilesSet = new Set(processedFiles); // Chuyển sang Set để kiểm tra nhanh hơn
		const newFiles = allFilesInDir.filter(
			(file) => !processedFilesSet.has(file)
		);
		return newFiles;
	} catch (error) {
		console.error('❌ Error reading directory or Redis for new files:', error);
		return []; // Trả về mảng rỗng nếu có lỗi
	}
};

// --- Hàm gán batch chính, giờ chạy trong vòng lặp ---
export const startBatchAssigner = async () => {
	console.log('🚀 Starting batch assignment loop...');
	try {
		await producer.connect();
		console.log('✅ Kafka Producer connected.');
	} catch (err) {
		console.error('❌ Failed to connect Kafka Producer:', err);
		return; // Không thể tiếp tục nếu không kết nối được producer
	}

	while (isRunning) {
		const fileQueue = await getNewFiles(); // Lấy danh sách file mới cần xử lý
		if (fileQueue.length === 0) {
			console.log(
				`🕒 No new files found. Waiting ${CHECK_INTERVAL / 1000}s...`
			);
			process.stdout.write(' W '); // Waiting symbol
			await delay(CHECK_INTERVAL);
			continue; // Quay lại đầu vòng lặp để kiểm tra lại
		}

		process.stdout.write('\n'); // New line when files found
		console.log(
			`📂 Found ${fileQueue.length} new files to process:`,
			fileQueue
		);

		// ---- Bắt đầu xử lý các file trong fileQueue hiện tại ----
		while (fileQueue.length > 0 && isRunning) {
			// Thêm kiểm tra isRunning ở đây nữa
			const currentFile = fileQueue.shift(); // Lấy và xóa file khỏi hàng đợi
			if (!currentFile) continue;

			console.log(`\n⏳ Processing file: ${currentFile}`);
			console.log('Remaining in current queue: ', fileQueue);

			const fileIdBase =
				path.basename(currentFile, '.txt');
			const filePath = path.join(DATA_DIR, currentFile);
			let lines;
			try {
				lines = fs
					.readFileSync(filePath, 'utf-8')
					.split('\n')
					.filter((line) => line.trim());
			} catch (err) {
				console.error(`❌ Error reading file ${currentFile}:`, err);
				logMessage(`Error reading file ${currentFile}: ${err.message}`);
				continue; // Bỏ qua file lỗi này trong hàng đợi hiện tại
			}

			if (lines.length === 0) {
				console.warn(
					`⚠️ File ${currentFile} is empty. Marking as processed.`
				);
				logMessage(`Skipped empty file: ${currentFile}`);
				await redis.sadd(PROCESSED_FILES_KEY, currentFile);
				continue;
			}

			// --- Logic chia chunk và gán worker (giữ nguyên phần lớn) ---
			const chunkSizeRaw = await redis.get('numBatches');
			const chunkSize = parseInt(chunkSizeRaw) || 500;
			if (await redis.llen('master:fileChunks') === 0) {
				// Nếu fileChunks rỗng thì chia chunk và gán worker
				for (let i = 0; i < lines.length; i += chunkSize) {
					const chunk = lines.slice(i, i + chunkSize);
					const chunkIndex = Math.floor(i / chunkSize);
					const chunkId = `${fileIdBase}_chunk_${chunkIndex}`;
					await redis.rpush('master:fileChunks', JSON.stringify({ chunkId, chunk }));
				}
			}
			// Nếu fileChunks không rỗng thì tiếp tục xử lý 
			console.log(`  -> Total Chunks: ${await redis.llen('master:fileChunks')}`);

			let fileProcessingSuccess = true; // Cờ đánh dấu file xử lý thành công

			while ((await redis.llen('master:fileChunks')) > 0 && isRunning) {
				let chosenWorker = null;
				let lockedWorkerId = null;

				const allWorkers = await redis.hgetall('worker:status');
				const readyWorkers = Object.entries(allWorkers)
					.filter(([_, status]) => status === '1')
					.map(([id]) => id);

				if (readyWorkers.length === 0) {
					process.stdout.write('.'); // Waiting for worker
					await delay(WORKER_POLL_INTERVAL);
					if (!isRunning) break; // Thoát nếu nhận tín hiệu dừng trong lúc đợi worker
					continue;
				}

				for (const workerId of readyWorkers) {
					if (!isRunning) break; // Thoát sớm nếu nhận tín hiệu dừng
					try {
						if (await acquireLock(workerId, 5000)) {
							lockedWorkerId = workerId;
							const currentStatus = await redis.hget('worker:status', workerId);
							if (currentStatus === '1') {
								chosenWorker = workerId;
								console.log(`   🔒 Locked worker ${chosenWorker}`);
								break;
							} else {
								console.warn(
									`   ⚠️ Worker ${workerId} status changed. Releasing lock.`
								);
								await releaseLock(workerId);
							}
						}
					} catch (error) {
						console.error(`Error processing worker ${workerId}:`, error);
					}
				}
				if (!isRunning) break; // Thoát vòng lặp gán chunk nếu nhận tín hiệu dừng

				if (chosenWorker) {
					const chunkToAssign = await redis.lpop('master:fileChunks');
					if (!chunkToAssign) {
						console.error('   ❌ Logic error: No more chunks to assign.');
						if (lockedWorkerId) await releaseLock(lockedWorkerId);
						fileProcessingSuccess = false; // Đánh dấu file thất bại
						break;
					}
					const { chunkId, chunk } = JSON.parse(chunkToAssign);
					await redis.hset('worker:status', chosenWorker, '0');
					console.log(`   🚦 Marked worker ${chosenWorker} as busy (0)`);

					const partitionRaw = await redis.hget(
						'worker:partition',
						chosenWorker
					);
					let partitions;
					try {
						if (!partitionRaw) throw new Error('No partition info found');
						partitions = JSON.parse(partitionRaw);
						if (!Array.isArray(partitions) || partitions.length === 0) {
							throw new Error('Invalid partition list');
						}
					} catch (err) {
						console.error(
							`   ❌ Partition error for ${chosenWorker}: ${err.message}`
						);
						await redis.hset('worker:status', chosenWorker, '0');
						if (lockedWorkerId) await releaseLock(lockedWorkerId);
						console.log(
							`   🔧 Reset worker ${chosenWorker} to ready (1) and released lock.`
						);
						await redis.lpush('master:fileChunks', chunkToAssign);
						chosenWorker = null;
						await delay(500);
						continue; // Thử tìm worker khác
					}

					// const partition = partitions[Math.floor(Math.random() * partitions.length)];
					const partition = partitions.at(0);
					try {
						console.log(
							`   📦 Assigning chunk ${chunkId} (${chunk.length} items) to ${chosenWorker} (partition ${partition})`
						);
						await redis.hset(
							'worker:batchInfo',
							chosenWorker,
							JSON.stringify({
								batchId: chunkId,
								total: chunk.length,
								assignedAt: Date.now(),
							})
						);
						await sendChunkToKafka(chosenWorker, chunkId, chunk, partition);
						console.log(`   🔄 Assigned chunk ${chunkId} to ${chosenWorker}`);

						// THÊM MỚI: Kiểm tra và áp dụng cập nhật đang chờ
						const needsUpdate = await redis.exists(`worker:needs:update:${chosenWorker}`);
						if (needsUpdate) {
							console.log(`⚠️ Worker ${chosenWorker} has pending updates that will be applied later`);
						}
					} catch (err) {
						console.error(
							`   ❌ Send/Assign error for chunk ${chunkId} to ${chosenWorker}:`,
							err
						);
						logMessage(
							`Failed assign/send chunk ${chunkId} to ${chosenWorker}: ${err.message}`
						);
						await redis.hset('worker:status', chosenWorker, '0');
						if (lockedWorkerId) await releaseLock(lockedWorkerId);
						console.log(
							`   🔧 Reset worker ${chosenWorker} to ready (1) and released lock due to send error.`
						);
						await redis.lpush('master:fileChunks', chunkToAssign);
						await delay(500); // Chờ chút trước khi thử lại
					}
				} else {
					process.stdout.write('~'); // Waiting for lock
					await delay(100);
					if (!isRunning) break; // Thoát nếu nhận tín hiệu dừng trong lúc đợi lock
				}
			} // --- Kết thúc vòng lặp gán chunk cho file hiện tại ---

			if (!isRunning) break; // Thoát vòng lặp xử lý file nếu nhận tín hiệu dừng

			// --- Đánh dấu file đã xử lý vào Redis SET ---
			if ((await redis.llen('master:fileChunks')) === 0 && fileProcessingSuccess) {
				try {
					await redis.sadd(PROCESSED_FILES_KEY, currentFile);
					console.log(
						`💾 Marked file "${currentFile}" as processed in Redis.`
					);
					logMessage('Batch file marked as processed: ' + currentFile);
				} catch (redisErr) {
					console.error(
						`❌ Error marking file "${currentFile}" as processed in Redis:`,
						redisErr
					);
					// Cân nhắc: Nếu lỗi ở đây, file này có thể bị xử lý lại lần sau.
				}
			} else if (!fileProcessingSuccess) {
				console.error(
					`❌ File "${currentFile}" processing failed. It will be retried later.`
				);
				logMessage(`File "${currentFile}" processing failed. Will retry.`);
			} else {
				console.warn(
					`⚠️ File "${currentFile}" processing interrupted. It will be retried later.`
				);
				logMessage(
					`File "${currentFile}" processing interrupted. Will retry.`
				);
			}
		} // --- Kết thúc vòng lặp xử lý các file trong fileQueue hiện tại ---
	} // --- Kết thúc vòng lặp chính (khi isRunning = false) ---

	console.log('🛑 Batch assignment loop stopped.');
	try {
		await producer.disconnect();
		console.log('🔌 Kafka Producer disconnected.');
	} catch (err) {
		console.error('❌ Error disconnecting Kafka Producer:', err);
	}
};

const sendChunkToKafka = async (workerId, batchId, ipList, partition) => {
	try {
		await producer.send({
			topic: process.env.KAFKA_TOPIC_NAME_MASTER,
			messages: [
				{
					key: workerId,
					partition: partition,
					value: JSON.stringify({
						id: workerId,
						batchId: batchId,
						data: ipList,
					}),
				},
			],
		});
	} catch (err) {
		console.error(
			`   ❌ Kafka send error for batch ${batchId} to worker ${workerId}:`,
			err
		);
		throw err; // Ném lỗi để hàm gọi xử lý
	}
};

// --- Xử lý tín hiệu dừng ---
const handleShutdown = async (signal) => {
	console.log(`\n✋ ${signal} received. Stopping batch assigner loop...`);
	isRunning = false;
	// Không cần gọi disconnect ở đây, vòng lặp sẽ tự thoát và disconnect
};

process.on('SIGTERM', () => handleShutdown('SIGTERM'));
process.on('SIGINT', () => handleShutdown('SIGINT')); // Ctrl+C
