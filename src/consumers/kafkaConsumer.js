import kafka from '../configs/kafkaConfig.js';
import redis from '../configs/redisConfig.js';
import { assignBatches } from '../producers/kafkaProducer.js';
import 'dotenv/config';
const consumer = kafka.consumer({
	groupId: 'master-group',
});

const checkWorkerStatus = () => {
	console.log('⏱️ Starting worker status monitor...');
	setInterval(async () => {
		try {
			const workers = await redis.hgetall('worker:status');
			const now = Date.now();
			// Duyệt qua từng worker
			for (const [workerId, status] of Object.entries(workers)) {
				// Nếu worker đã offline thì bỏ qua
				if (status !== '0') continue;
				// Lấy thời điểm nhận message cuối cùng của worker
				const lastSeen = await redis.get(`lastSeen:${workerId}`);
				// Nếu quá 20s không nhận được message => đánh dấu offline
				console.log(
					`⏳ Checking worker ${workerId}, status=${status}, lastSeen=${lastSeen}`
				);
				if (!lastSeen || now - parseInt(lastSeen) > 20000) {
					console.warn(`⚠️ Worker ${workerId} has offline!`);
					await redis.hset('worker:status', workerId, '-1');
				}
			}
		} catch (err) {
			console.error('❌ Error in offline check: ', err);
		}
	}, 5000);
};

export const runConsumer = async () => {
	await consumer.connect();

	console.log('Consumer connected');

	await consumer.subscribe({
		topics: [
			process.env.KAFKA_TOPIC_NAME_WORKER,
			process.env.KAFKA_TOPIC_NAME_WORKER_FREE,
		],
		fromBeginning: false,
	});

	await consumer.run({
		eachMessage: async ({ topic, message }) => {
			if (topic === process.env.KAFKA_TOPIC_NAME_WORKER_FREE) {
				const { id: workerId, partitions } = JSON.parse(
					message.value.toString()
				);
				// Store worker status and partition
				await redis.hset('worker:status', workerId, '1');
				await redis.hset(
					'worker:partition',
					workerId,
					JSON.stringify(partitions)
				);
				console.log(`🔧 Worker ${workerId} is free now`);
			} else {
				const {
					id: workerId,
					batchId,
					processing,
					total,
				} = JSON.parse(message.value.toString());

				if (isNaN(parseInt(processing)) || isNaN(parseInt(total))) {
					console.error(
						`❌ Invalid processed or total attributes for batch ${batchId}`
					);
					return;
				}
				await redis.hset('worker:processing', batchId, processing);
				await redis.set(`lastSeen:${workerId}`, Date.now());
				console.log(`🚀 [${workerId}] Processing ${processing}/${total}`);
				if (parseInt(processing) === parseInt(total)) {
					await redis.hset('worker:status', workerId, '1');
					await assignBatches(); // Assign new batch to worker
				}
			}
		},
	});
	checkWorkerStatus();
};
