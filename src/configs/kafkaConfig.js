import { Kafka } from 'kafkajs';
import 'dotenv/config';
import { randomUUID } from 'crypto';
class KafkaManager {
	constructor() {
		this.kafka = new Kafka({
			clientId: 'kafka-master',
			brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
		});
		this.producer = this.kafka.producer();
		this.consumer = this.kafka.consumer({
			groupId: process.env.KAFKA_GROUP_ID,
		});
		this.workerPartitionCounters = new Map();
		const admin = this.kafka.admin();
		admin
			.connect()
			.then(() => {
				admin.createTopics({
					topics: [
						{
							topic: process.env.KAFKA_MASTER_SEND_TOPIC,
							numPartitions: 10,
							replicationFactor: 1,
						},
						{
							topic: process.env.KAFKA_WORKER_SEND_TOPIC,
							numPartitions: 1,
							replicationFactor: 1,
						},
						{
							topic: process.env.KAFKA_WORKER_CONNECTION_TOPIC,
							numPartitions: 1,
							replicationFactor: 1,
						},
					],
				});
			})
			.catch((err) => {
				console.error(err);
			});
	}

	async connect() {
		await this.producer.connect();
		await this.consumer.connect();
		await this.consumer.subscribe({
			topics: [
				process.env.KAFKA_WORKER_SEND_TOPIC,
				process.env.KAFKA_WORKER_CONNECTION_TOPIC,
			],
		});
	}

	async disconnect() {
		await this.producer.disconnect();
		await this.consumer.disconnect();
	}

	// Gửi batch đến worker
	async sendBatch(workerId, ipBatch, fileName, startPos, endPos, state) {
		const batchId = randomUUID();
		const message = {
			id: workerId,
			batchId,
			data: ipBatch,
		};

		const workerInfo = await state.getWorkerInfo(workerId);
		const partitions = workerInfo.partitions;
		if (!partitions.length) {
			console.error(`Worker ${workerId} has no partitions assigned`);
			return null;
		}

		const counter = this.workerPartitionCounters.get(workerId) || 0;
		const partition = partitions[counter % partitions.length];
		this.workerPartitionCounters.set(workerId, counter + 1);

		await this.producer.send({
			topic: process.env.KAFKA_MASTER_SEND_TOPIC,
			messages: [{ key: workerId, value: JSON.stringify(message), partition }],
		});

		await state.saveBatch(
			batchId,
			fileName,
			startPos,
			endPos,
			'doing',
			workerId
		);
		console.log(
			`Sent batch ${batchId} to worker ${workerId} on partition ${partition}`
		);
		return batchId;
	}

	// Xử lý tin nhắn từ worker
	async runConsumer(state) {
		await this.consumer.run({
			eachMessage: async ({ topic, message }) => {
				const data = JSON.parse(message.value.toString());
				const workerId = data.id;

				if (topic === process.env.KAFKA_WORKER_CONNECTION_TOPIC) {
					if (data.status === 'new') {
						// Thêm worker mới với partitions
						await state.addWorker(workerId, data.partitions || []);
						console.log(
							`Worker ${workerId} connected with partitions: ${
								data.partitions || []
							}`
						);
						// Đảm bảo worker mới sẵn sàng nếu không có batch đang xử lý
						const workerInfo = await state.getWorkerInfo(workerId);
						if (workerInfo.pendingBatches === 0) {
							console.log(`Worker ${workerId} is ready to receive batches`);
						}
					} else if (data.status === 'done') {
						await state.updateWorkerActivity(workerId);
						console.log(`Worker ${workerId} is ready for new batch`);
					}
				} else if (topic === process.env.KAFKA_WORKER_SEND_TOPIC) {
					const { batchId, processing, total } = data;
					console.log(
						`Worker ${workerId} processing batch ${batchId}: ${processing}/${total}`
					);
					await state.updateWorkerActivity(workerId); // Cập nhật hoạt động khi nhận kết quả
					if (processing === total) {
						await state.updateBatchStatus(batchId, 'success');
						console.log(`Batch ${batchId} completed by worker ${workerId}`);
					}
				}
			},
		});
	}
}

export default KafkaManager;
