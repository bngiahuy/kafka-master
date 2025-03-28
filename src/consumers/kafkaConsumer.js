import kafka from '../configs/kafkaConfig.js';
import redis from '../configs/redisConfig.js';
import 'dotenv/config';
import logMessage from '../utils/logger.js';
import { releaseLock } from '../utils/helper.js'; // Đảm bảo import đúng

const consumer = kafka.consumer({
	groupId: 'master-group',
	metadataMaxAge: 60000, // 1 phút
	allowAutoTopicCreation: true,
	retry: {
		initialRetryTime: 100,
		retries: 8,
	},
	sessionTimeout: 30000,
	heartbeatInterval: 3000,
});

// --- Worker Timeout Check --- (Giữ nguyên hoặc cải tiến nếu cần)
const checkWorkerStatus = () => {
	console.log('⏱️ Starting worker status monitor...');
	const intervalId = setInterval(async () => {
		// Lưu intervalId để có thể clear
		try {
			const workers = await redis.hgetall('worker:status');
			const now = Date.now();

			if (Object.keys(workers).length === 0) {
				// console.log("Monitor: No workers registered.");
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

				const estimatedProcessingTime = (batchInfo.total || 0) * 300 + 30000; // 0.1s/item
				const timeSinceAssigned = now - batchInfo.assignedAt;

				// Kiểm tra thêm lastSeen để chắc chắn worker còn hoạt động
				const lastSeenRaw = await redis.get(`lastSeen:${workerId}`);
				const lastSeenTime = lastSeenRaw ? parseInt(lastSeenRaw, 10) : 0;
				const timeSinceLastSeen = lastSeenTime ? now - lastSeenTime : Infinity; // Nếu chưa thấy -> coi như vô hạn

				// Coi worker là timeout nếu thời gian kể từ khi gán VƯỢT QUÁ thời gian ước tính
				// VÀ thời gian kể từ lần cuối thấy hoạt động cũng VƯỢT QUÁ timeout (hoặc chưa thấy bao giờ)
				if (
					timeSinceAssigned > estimatedProcessingTime &&
					timeSinceLastSeen > estimatedProcessingTime
				) {
					console.warn(
						`⚠️ Worker ${workerId} timeout detected! Assigned ${timeSinceAssigned / 1000
						}s ago, last seen ${timeSinceLastSeen / 1000}s ago. Resetting...`
					);
					logMessage(`Worker ${workerId} timeout. Resetting.`);
					// Xóa thông tin liên quan đến worker này
					const multi = redis.multi();
					multi.hdel('worker:status', workerId);
					multi.hdel('worker:partition', workerId);
					multi.hdel('worker:batchInfo', workerId);
					multi.del(`lastSeen:${workerId}`);
					multi.hdel('worker:processing', batchInfo.batchId); // Xóa tiến trình của batchId nếu có
					await releaseLock(workerId); // Quan trọng: giải phóng lock
					console.log(`🧹 Cleaned up timeout worker ${workerId}.`);
					await multi.exec();

					// Skip batch id này
				}
			}
		} catch (error) {
			console.error('❌ Error in worker status monitor:', error);
		}
	}, 10000); // Chạy mỗi 10 giây

	return () => clearInterval(intervalId); // Trả về hàm để dừng interval
};


export const runConsumer = async () => {
	let stopMonitoring = null; // Biến để giữ hàm dừng monitor
	try {
		await consumer.connect();
		console.log('✅ Consumer connected');

		await consumer.subscribe({
			topics: [
				process.env.KAFKA_TOPIC_NAME_WORKER_FREE, // Worker registration/ready
				process.env.KAFKA_TOPIC_NAME_WORKER, // Progress updates
			],
			fromBeginning: false, // Thường không cần xử lý lại message cũ khi consumer khởi động lại
		});
		console.log(
			`👂 Consumer subscribed to topics: ${process.env.KAFKA_TOPIC_NAME_WORKER}, ${process.env.KAFKA_TOPIC_NAME_WORKER_FREE}`
		);


		await consumer.run({
			// Để giảm số lượng message xử lý đồng thời, bạn có thể thay đổi giá trị này
			// partitionsConsumedConcurrently: 10, // Điều chỉnh dựa trên giới hạn API
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

					// Kiểm tra xem worker có đang xử lý batch nào không
					const batchInfoRaw = await redis.hget('worker:batchInfo', workerId);

					if (workerStatus === 'done') {
						if (!batchInfoRaw) {
							// Nếu không có batch đang xử lý, an toàn để set status = 1
							console.log(`🆓 Worker ${workerId} is free and ready.`);
							await redis.hset('worker:status', workerId, '1');
							await redis.hdel('worker:batchInfo', workerId);
							await releaseLock(workerId);
						} else {
							// Nếu còn batch đang xử lý, log warning
							console.warn(`⚠️ Worker ${workerId} reported done but still has active batch`);
						}
					} else if (workerStatus === 'new') {
						const { partitions } = data;
						if (!partitions) {
							console.warn(`⚠️ Worker ${workerId} sent 'new' status without partitions info`);
							return;
						}

						console.log(`🔄 Worker ${workerId} updating partitions after rebalance:`, partitions);

						try {

							// Cập nhật status và partition mới
							await redis.hset('worker:status', workerId, '1');
							await redis.hset('worker:partition', workerId, JSON.stringify(partitions));
							await redis.hdel('worker:batchInfo', workerId);
							await releaseLock(workerId);

							// Kiểm tra và xóa thông tin batch cũ nếu có


							console.log(`✅ Successfully updated partitions for worker ${workerId}`);
						} catch (error) {
							console.error(`❌ Failed to update worker ${workerId} after rebalance:`, error);
							// Có thể thêm retry logic ở đây nếu cần
						}
					}
				}
				// --- Xử lý Worker báo cáo tiến trình ---
				else {
					const {
						id: workerId,
						batchId,
						processing,
						total,
						error, // Optional error field from worker
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
					// console.log(`   -> Updated lastSeen for ${workerId}`);

					// --- Log progress ---
					console.log(
						`${workerId} progress ${batchId}: ${processedCount}/${totalCount}`
					);
					logMessage(
						`${workerId} progress ${batchId}: ${processedCount}/${totalCount}`
					);
					// Có thể lưu tiến trình vào Redis nếu cần theo dõi chi tiết, nhưng không bắt buộc
					multi.hset('worker:processing', batchId, processedCount);

					// --- Check for errors reported by worker ---
					if (error) {
						console.error(
							`❌ Worker ${workerId} reported error for batch ${batchId}:`,
							error
						);
						logMessage(
							`Worker ${workerId} error on batch ${batchId}: ${error}`
						);
						// Worker báo lỗi -> Coi như xong việc (lỗi), reset worker
						multi.set(`worker:status`, workerId, '1');
						await releaseLock(workerId); // Giải phóng lock
						multi.hdel('worker:batchInfo', workerId); // Xóa thông tin batch đang làm
						await multi.exec();
						console.log(
							`   -> Worker ${workerId} reset to ready (1) due to reported error.`
						);
						// TODO: Xử lý batchId bị lỗi (ghi log, đưa vào hàng đợi lỗi, ...)
						return; // Dừng xử lý message này
					}
					// --- Check if batch is completed ---
					if (processedCount === totalCount) {
						console.log(
							`✅ Worker ${workerId} completed batch ${batchId} (${processedCount}/${totalCount}).`
						);
						// logMessage(`Worker ${workerId} completed batch ${batchId}`);
						multi.hdel('worker:batchInfo', workerId); // Xóa thông tin batch đã xong
						console.log(
							`   -> Worker ${workerId} status set to 1 (Ready) and lock released.`
						);


					}
					await multi.exec();
				}
			},
		});
		stopMonitoring = checkWorkerStatus();


		// Giữ consumer chạy
		console.log('⏳ Consumer is running. Waiting for messages...');
		// Để ngăn hàm kết thúc ngay lập tức, bạn có thể dùng một promise không bao giờ resolve
		// Hoặc dựa vào việc process không tự thoát
		// await new Promise(() => {});
	} catch (error) {
		console.error('❌ Fatal error in consumer:', error);
		if (stopMonitoring) {
			console.log('Stopping worker monitor due to consumer error...');
			stopMonitoring(); // Dừng interval nếu có lỗi
		}
		// Cân nhắc việc cố gắng kết nối lại hoặc thoát process
		// await consumer.disconnect(); // Thử ngắt kết nối
		// process.exit(1);
	}

	// Xử lý tín hiệu dừng cho consumer
	const errorTypes = ['unhandledRejection', 'uncaughtException'];
	const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

	errorTypes.forEach((type) => {
		process.on(type, async (err) => {
			try {
				console.log(`🔥 Process ${type}: ${err.message}`);
				if (stopMonitoring) stopMonitoring();
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
				await consumer.disconnect();
				console.log('Consumer disconnected gracefully.');
			} finally {
				process.kill(process.pid, type); // Đảm bảo process kết thúc đúng cách
			}
		});
	});
};
