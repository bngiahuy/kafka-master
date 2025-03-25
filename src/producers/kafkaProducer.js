import 'dotenv/config';
import kafka from '../configs/kafkaConfig.js';
import fs from 'fs';
import path from 'path';
import redis from '../configs/redisConfig.js';
import { randomUUID } from 'crypto';
import logMessage from '../utils/logger.js';
const producer = kafka.producer();

const BATCH_DIR = './input_ip_data';
const BATCH_FILES = fs.readdirSync(BATCH_DIR).filter((f) => f.endsWith('.txt'));
let batchQueue = [...BATCH_FILES];

const acquireLock = async (workerId, timeout = 1000) => {
	const lockKey = `lock:worker:${workerId}`;
	const startTime = Date.now();
	const result = await redis.set(lockKey, 'locked', 'NX', 'PX', timeout);
	const endTime = Date.now();
	console.log(
		`🚀 ~ acquireLock ~ worker: ${workerId}, result: ${result}, time: ${
			endTime - startTime
		}ms`
	);
	return result === 'OK';
};

const releaseLock = async (workerId) => {
	const lockKey = `lock:worker:${workerId}`;
	await redis.del(lockKey);
};

export const assignBatches = async () => {
	// Tiếp tục xử lý cho đến khi hàng đợi file trống
	while (batchQueue.length > 0) {
		// --- Lấy file tiếp theo từ hàng đợi ---
		const currentBatchFile = batchQueue.shift();
		const batchIdBase = path.basename(currentBatchFile, '.txt') || randomUUID();
		const filePath = path.join(BATCH_DIR, currentBatchFile);
		const lines = fs
			.readFileSync(filePath, 'utf-8')
			.split('\n')
			.filter((line) => line.trim());

		// --- Lấy chunkSize từ Redis (mặc định 1000 nếu parse lỗi) ---
		const chunkSizeRaw = await redis.get('numBatches');
		// console.log('🚀 ~ assignBatches ~ chunkSizeRaw:', chunkSizeRaw);
		const chunkSize = parseInt(chunkSizeRaw) || 1000;

		// --- Chia file thành các chunk ---
		const fileChunks = [];
		for (let i = 0; i < lines.length; i += chunkSize) {
			const chunk = lines.slice(i, i + chunkSize);
			const chunkId = `${batchIdBase}_${i / chunkSize}`;
			fileChunks.push({ chunkId, chunk });
		}

		console.log(`\nBắt đầu xử lý file: ${currentBatchFile}`);
		console.log(`  -> Tổng số chunk: ${fileChunks.length}`);

		// --- Gửi song song các chunk cho các worker sẵn sàng ---
		const chunkPromises = fileChunks.map(async ({ chunkId, chunk }) => {
			let chosenWorker = null;
			do {
				// Lấy danh sách worker và trạng thái
				const allWorkers = await redis.hgetall('worker:status');
				// Lọc ra các worker sẵn sàng
				const readyWorkers = Object.entries(allWorkers)
					.filter(([_, status]) => status === '1')
					.map(([id]) => id);

				if (readyWorkers.length > 0) {
					// Thử chiếm khóa cho từng worker sẵn sàng
					for (const worker of readyWorkers) {
						console.log('🚀 ~ chunkPromises ~ worker:', worker);
						if (await acquireLock(worker)) {
							chosenWorker = worker;

							// Đặt trạng thái worker thành "busy"
							await redis.hset('worker:status', chosenWorker, '0');
							const currentWorkerStatus = await redis.hget(
								'worker:status',
								chosenWorker
							);
							console.log(
								`🚀 ~ ${chosenWorker} ~ currentWorkerStatus: ${currentWorkerStatus} `
							);
							break;
						}
					}
				}

				if (!chosenWorker) {
					// Không có worker nào sẵn sàng hoặc không chiếm được khóa, chờ 100ms rồi thử lại
					await new Promise((r) => setTimeout(r, 100));
				}
			} while (!chosenWorker);

			// Lấy partition cho worker
			const partitionRaw = await redis.hget('worker:partition', chosenWorker);
			if (!partitionRaw) {
				console.warn(`⚠️ No partition assigned to ${chosenWorker}`);
				await releaseLock(chosenWorker);
				await redis.hset('worker:status', chosenWorker, '1');
				return;
			}

			let partitions;
			try {
				partitions = JSON.parse(partitionRaw);
			} catch (err) {
				console.error(`❌ Error parsing partitions for ${chosenWorker}:`, err);
				await releaseLock(chosenWorker);
				await redis.hset('worker:status', chosenWorker, '1');
				return;
			}

			if (!Array.isArray(partitions) || partitions.length === 0) {
				console.warn(`⚠️ Invalid partition list for ${chosenWorker}`);
				await releaseLock(chosenWorker);
				await redis.hset('worker:status', chosenWorker, '1');
				return;
			}

			// Chọn ngẫu nhiên 1 partition
			const partition =
				partitions[Math.floor(Math.random() * partitions.length)];

			try {
				// Gửi chunk lên Kafka
				await runProducer(chosenWorker, chunkId, chunk, partition);
				console.log(
					`✅ Sent batch ${chunkId} to ${chosenWorker} via partition ${partition} of topic ${process.env.KAFKA_TOPIC_NAME_MASTER}`
				);
			} catch (err) {
				console.error(
					`❌ Failed to assign chunk ${chunkId} to ${chosenWorker}:`,
					err
				);
			} finally {
				// Giải phóng khóa và trả worker về trạng thái sẵn sàng
				await releaseLock(chosenWorker);
				await redis.hset('worker:status', chosenWorker, '1');
			}
		});

		// Đợi toàn bộ các chunk của file hiện tại xong mới sang file kế tiếp
		await Promise.all(chunkPromises);
		console.log(`✅ Hoàn thành file: ${currentBatchFile}\n`);

		// --- Ghi log ra file "processed_batches.log" ---
		try {
			logMessage('Batch đã xử lý: ' + currentBatchFile);
			logMessage(
				`📝 Đã ghi log file "${currentBatchFile}" vào processed_batches.log`
			);
		} catch (err) {
			console.error('❌ Lỗi khi ghi log processed_batches.log:', err);
		}
	}
	console.log('✅ Tất cả file trong batchQueue đã được xử lý!');
};

const runProducer = async (workerId, batchId, ipList, partition) => {
	await producer.connect();
	await redis.hset('worker:total', batchId, ipList.length);
	try {
		await producer.send({
			topic: process.env.KAFKA_TOPIC_NAME_MASTER,
			messages: [
				{
					key: workerId,
					partition,
					value: JSON.stringify({
						id: workerId,
						batchId,
						data: ipList,
					}),
				},
			],
		});
		console.log(
			`✅ Sent batch ${batchId} to ${workerId} via partition ${partition} of topic ${process.env.KAFKA_TOPIC_NAME_MASTER}`
		);
	} catch (err) {
		console.error(`❌ Failed to send to ${workerId}:`, err);
		throw err;
	}
	await producer.disconnect();
};

export default runProducer;
