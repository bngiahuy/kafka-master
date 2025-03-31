import kafka from '../configs/kafkaConfig.js';
import redis from '../configs/redisConfig.js';
import 'dotenv/config';
import { logMessage, logBatchFailure, logBatchSuccess } from '../utils/logger.js';
import { releaseLock, acquireLock } from '../utils/helper.js'; // Đảm bảo import đúng
import fs from 'fs';
import path from 'path';
import { startWorkerUpdater } from '../utils/workerManager.js';

const consumer = kafka.consumer({
	groupId: 'master-group',
	metadataMaxAge: 60000, // 1 phút
	allowAutoTopicCreation: true,
	retry: {
		initialRetryTime: 100,
		retries: 8,
	},
	sessionTimeout: 60000,
	heartbeatInterval: 10000,
	rebalanceTimeout: 60000,
});

const getChunkById = async (chunkId) => {
	// Chunk id = vietnam_ips_1_chunk_1.txt, -> fileName = vietnam_ips_1.txt
	const fileName = chunkId.split('_chunk_')[0] + '.txt';
	const DATA_PATH = process.env.CLIENT_DATA_PATH + '/input';
	const filePath = path.join(DATA_PATH, fileName);
	const lines = fs
		.readFileSync(filePath, 'utf-8')
		.split('\n')
		.filter((line) => line.trim());

	const chunkIndex = chunkId.split('_chunk_')[1].split('.')[0];
	const chunkSizeRaw = await redis.get('numBatches');
	const chunkSize = parseInt(chunkSizeRaw, 10) || 500;
	const start = chunkIndex * chunkSize;
	return lines.slice(start, start + chunkSize);
}

// --- Worker Timeout Check --- (Giữ nguyên hoặc cải tiến nếu cần)
const checkWorkerStatus = () => {
	console.log('⏱️ Starting worker status monitor...');
	const intervalId = setInterval(async () => {
		// Lưu intervalId để có thể clear
		try {
			const workers = await redis.hgetall('worker:status');
			const now = Date.now();

			if (Object.keys(workers).length === 0) {
				console.log("Monitor: No workers registered.");
				return;
			}

			for (const [workerId, status] of Object.entries(workers)) {
				if (status === '1') continue; // Bỏ qua worker đang sẵn sàng

				const batchInfoRaw = await redis.hget('worker:batchInfo', workerId);
				// console.log(`Monitor check ${workerId}: Status=${status}, BatchInfo=${batchInfoRaw}`);

				if (!batchInfoRaw) {
					console.warn(
						`⚠️ Worker ${workerId} is busy (0) but has no batchInfo. Resetting.`
					);
					await redis.hset('worker:status', workerId, '1'); // Reset về sẵn sàng
					await releaseLock(workerId); // Cố gắng giải phóng lock phòng trường hợp còn sót
					continue;
				}

				let batchInfo;
				try {
					batchInfo = JSON.parse(batchInfoRaw);
					if (
						!batchInfo ||
						typeof batchInfo.total !== 'number' ||
						typeof batchInfo.assignedAt !== 'number'
					) {
						console.warn(
							`⚠️ Worker ${workerId} has invalid batchInfo: ${batchInfoRaw}. Resetting.`
						);
						await redis.hset('worker:status', workerId, '1');
						await releaseLock(workerId);
						await redis.hdel('worker:batchInfo', workerId); // Xóa thông tin batch lỗi
						continue;
					}
				} catch (e) {
					console.warn(
						`⚠️ Worker ${workerId} failed to parse batchInfo: ${batchInfoRaw}. Resetting. Error: ${e.message}`
					);
					await redis.hset('worker:status', workerId, '1');
					await releaseLock(workerId);
					await redis.hdel('worker:batchInfo', workerId);
					continue;
				}

				const estimatedProcessingTime = (process.env.WORKER_TIMEOUT || 80) * 1000;
				const timeSinceAssigned = now - batchInfo.assignedAt;

				// Kiểm tra thêm lastSeen để chắc chắn worker còn hoạt động
				const lastSeenRaw = await redis.get(`lastSeen:${workerId}`);
				const lastSeenTime = lastSeenRaw ? parseInt(lastSeenRaw, 10) : 0;
				const timeSinceLastSeen = lastSeenTime > 0 ? now - lastSeenTime : Infinity; // Nếu chưa thấy -> coi như vô hạn

				// Coi worker là timeout nếu thời gian kể từ khi gán VƯỢT QUÁ thời gian ước tính
				// VÀ thời gian kể từ lần cuối thấy hoạt động cũng VƯỢT QUÁ timeout (hoặc chưa thấy bao giờ)
				if (
					timeSinceAssigned > estimatedProcessingTime ||
					timeSinceLastSeen > estimatedProcessingTime
				) {
					console.warn(
						`⚠️ Worker ${workerId} timeout detected! Assigned ${timeSinceAssigned / 1000
						}s ago, last seen ${timeSinceLastSeen / 1000}s ago. Resetting...`
					);
					logMessage(`Worker ${workerId} timeout. Resetting.`);
					// Xóa thông tin liên quan đến worker này
					// Skip batch id này, ghi lại lỗi vào log
					const multi = redis.multi();
					multi.hdel('worker:status', workerId);
					multi.hdel('worker:partition', workerId);
					multi.hdel('worker:batchInfo', workerId);
					multi.del(`lastSeen:${workerId}`);
					multi.hdel('worker:processing', batchInfo.batchId); // Xóa tiến trình của batchId nếu có
					console.log(`🧹 Cleaned up timeout worker ${workerId}.`);
					await multi.exec();
					await releaseLock(workerId); // Quan trọng: giải phóng lock
					logBatchFailure({
						workerId,
						batchId: batchInfo.batchId,
					}, 'Worker timeout');

					// Thêm lại chunk vào queue
					const chunk = await getChunkById(batchInfo.batchId);
					await redis.rpush('master:fileChunks', JSON.stringify({ chunkId: batchInfo.batchId, chunk }));
					console.log(`🔄 Added chunk ${batchInfo.batchId} back to queue, size: ${chunk.length}`);
				}
			}
		} catch (error) {
			console.error('❌ Error in worker status monitor:', error);
		}
	}, 10000); // Chạy mỗi 10 giây

	return () => clearInterval(intervalId); // Trả về hàm để dừng interval
};


export const runConsumer = async () => {
	let stopMonitoring = null;
	let stopWorkerUpdater = null; // Thêm biến để giữ hàm dừng worker updater

	try {
		await consumer.connect();
		console.log('✅ Consumer connected');

		await consumer.subscribe({
			topics: [
				process.env.KAFKA_TOPIC_NAME_WORKER_FREE,
				process.env.KAFKA_TOPIC_NAME_WORKER,
			],
			fromBeginning: false,
		});
		console.log(
			`👂 Consumer subscribed to topics: ${process.env.KAFKA_TOPIC_NAME_WORKER}, ${process.env.KAFKA_TOPIC_NAME_WORKER_FREE}`
		);

		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				console.log(`\n📩 Received message on topic "${topic}", partition ${partition}`);
				let data = JSON.parse(message.value.toString());

				// --- Xử lý Worker đăng ký hoặc báo sẵn sàng ---
				if (topic === process.env.KAFKA_TOPIC_NAME_WORKER_FREE) {
					const { id: workerId, status: workerStatus } = data;
					if (!workerId || !workerStatus) {
						console.warn('⚠️ Received invalid WORKER_FREE message:', data);
						return;
					}

					// Đưa vào hàng đợi Redis nếu là cập nhật partition
					if (workerStatus === 'new' && data.partitions) {
						// Thay vì cố gắng lock, lưu thông tin vào Redis để xử lý sau
						const updateData = {
							partitions: JSON.stringify(data.partitions),
							timestamp: Date.now(),
							status: workerStatus
						};

						await redis.hmset(`worker:pending:update:${workerId}`, updateData);
						await redis.set(`worker:needs:update:${workerId}`, '1', 'EX', 600);

						console.log(`📝 Queued partition update for worker ${workerId}`);

						// Vẫn cập nhật lastSeen để biết worker còn hoạt động
						await redis.set(`lastSeen:${workerId}`, Date.now(), 'EX', 60 * 5);
						return;
					}

					// Xử lý các trạng thái khác bình thường (như done)
					const hasLock = await acquireLock(workerId, 5000);
					if (!hasLock) {
						console.log(`⚠️ Cannot acquire lock for worker ${workerId} - will queue update`);

						// Đưa vào hàng đợi nếu không lấy được lock
						const updateData = {
							status: workerStatus,
							timestamp: Date.now()
						};

						await redis.hmset(`worker:pending:update:${workerId}`, updateData);
						await redis.set(`worker:needs:update:${workerId}`, '1', 'EX', 600);
						return;
					}

					try {
						if (workerStatus === 'done') {
							const multi = redis.multi();
							multi.hset('worker:status', workerId, '1');
							await multi.exec();
						}
						// 'new' đã được xử lý ở trên
					} finally {
						await releaseLock(workerId);
					}
				}
				// --- Xử lý Worker báo cáo tiến trình ---
				else {
					const {
						id: workerId,
						batchId,
						processing,
						total,
					} = data;

					// --- Validate data ---
					if (
						!workerId ||
						!batchId ||
						processing === undefined ||
						total === undefined
					) {
						console.warn('⚠️ Received invalid WORKER progress message:', data);
						return;
					}
					const processedCount = parseInt(processing, 10);
					const totalCount = parseInt(total, 10);
					if (isNaN(processedCount) || isNaN(totalCount)) {
						console.error(
							`❌ Invalid processing/total attributes for batch ${batchId} from worker ${workerId}:`,
							data
						);
						return;
					}

					const multi = redis.multi();
					// --- Update last seen time ---
					multi.set(`lastSeen:${workerId}`, Date.now(), 'EX', 60 * 5); // Cập nhật và tự hết hạn sau 5 phút nếu không có cập nhật mới

					// --- Log progress ---
					console.log(
						`${workerId} progress ${batchId}: ${processedCount}/${totalCount}`
					);
					logMessage(
						`${workerId} progress ${batchId}: ${processedCount}/${totalCount}`
					);
					multi.hset('worker:processing', batchId, processedCount);
					// --- Check if batch is completed ---
					if (processedCount === totalCount) {
						console.log(
							`✅ Worker ${workerId} completed batch ${batchId} (${processedCount}/${totalCount}).`
						);
						logBatchSuccess({
							workerId,
							batchId,
							processedCount,
							totalCount,
						});
						multi.hdel('worker:batchInfo', workerId); // Xóa thông tin batch đã xong
						console.log(
							`   -> Worker ${workerId} status set to 1 (Ready) and lock released.`
						);
					}
					await multi.exec();
				}
			},
		});

		// Khởi động các monitoring
		stopMonitoring = checkWorkerStatus();
		stopWorkerUpdater = startWorkerUpdater(); // Khởi động worker updater

		console.log('⏳ Consumer is running. Waiting for messages...');
	} catch (error) {
		console.error('❌ Fatal error in consumer:', error);
		if (stopMonitoring) stopMonitoring();
		if (stopWorkerUpdater) stopWorkerUpdater(); // Dừng worker updater nếu có lỗi
	}

	// Xử lý tín hiệu dừng cho consumer
	const errorTypes = ['unhandledRejection', 'uncaughtException'];
	const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

	errorTypes.forEach((type) => {
		process.on(type, async (err) => {
			try {
				console.log(`🔥 Process ${type}: ${err.message}`);
				if (stopMonitoring) stopMonitoring();
				if (stopWorkerUpdater) stopWorkerUpdater(); // Dừng worker updater khi có lỗi
				await consumer.disconnect();
				console.log('Consumer disconnected on error.');
				process.exit(1);
			} catch (_) {
				process.exit(1);
			}
		});
	});

	signalTraps.forEach((type) => {
		process.once(type, async () => {
			try {
				console.log(`✋ Signal ${type} received. Shutting down consumer...`);
				if (stopMonitoring) stopMonitoring();
				if (stopWorkerUpdater) stopWorkerUpdater(); // Dừng worker updater khi shutdown
				await consumer.disconnect();
				console.log('Consumer disconnected gracefully.');
			} finally {
				process.kill(process.pid, type); // Đảm bảo process kết thúc đúng cách
			}
		});
	});
};
