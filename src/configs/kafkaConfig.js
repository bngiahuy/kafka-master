import { Kafka } from 'kafkajs';
import 'dotenv/config';
import { randomUUID } from 'crypto';
import logger from '../utils/logger.js';

class KafkaManager {
	constructor() {
		this.kafka = new Kafka({
			clientId: 'kafka-master',
			brokers: [
				process.env.KAFKA_BROKER_ADDRESS + ':' + process.env.KAFKA_BROKER_PORT,
			],
		});
		this.producer = this.kafka.producer();
		this.consumer = this.kafka.consumer({
			groupId: process.env.KAFKA_GROUP_ID,
		});
		this.isProducerConnected = false;
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
						{
							topic: process.env.KAFKA_MASTER_HEARTBEAT_TOPIC,
							numPartitions: 1,
							replicationFactor: 1,
						},
					],
				});
			})
			.catch((err) => {
				console.error('Error creating topics: ', err);
				logger.error('Error creating topics: ', err);
			});
	}

	async connect() {
		if (!this.isProducerConnected) {
			await this.producer.connect();
			this.isProducerConnected = true;
		}
		await this.consumer.connect();
		await this.consumer.subscribe({
			topics: [
				process.env.KAFKA_WORKER_SEND_TOPIC,
				process.env.KAFKA_WORKER_CONNECTION_TOPIC,
			],
		});
	}

	async disconnect() {
		if (this.isProducerConnected) {
			await this.producer.disconnect();
			this.isProducerConnected = false;
		}
		await this.consumer.disconnect();
	}

	async increaseMasterPartition(newPartitionSize) {
		const admin = this.kafka.admin();
		await admin.connect().catch((err) => {
			console.error(
				'Error connecting to Kafka while updating partitions. Error: ',
				err
			);
			return;
		});

		await admin
			.createPartitions({
				timeout: 5000,
				topicPartitions: [
					{
						topic: process.env.KAFKA_MASTER_SEND_TOPIC,
						count: newPartitionSize,
					},
				],
			})
			.catch((err) => {
				console.error('Error updating partitions: ', err);
				return;
			});

		const topics = await admin.listTopics();
		const partitions = await Promise.all(
			topics.map(async (topic) => {
				const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
				return {
					topic,
					partitions: metadata.topics[0].partitions.length,
				};
			})
		);
		return partitions;
	}

	async getPartitions() {
		const admin = this.kafka.admin();
		await admin.connect().catch((err) => {
			console.error(
				'Error connecting to Kafka while getting partitions. Error: ',
				err
			);
			return;
		});

		const topics = await admin.listTopics();
		const partitions = await Promise.all(
			topics.map(async (topic) => {
				const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
				return {
					topic,
					partitions: metadata.topics[0].partitions.length,
				};
			})
		);
		return partitions;
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
		logger.info(
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
						logger.info(
							`Worker ${workerId} connected with partitions: ${
								data.partitions || []
							}`
						);
						// Đảm bảo worker mới sẵn sàng nếu không có batch đang xử lý
						const workerInfo = await state.getWorkerInfo(workerId);
						if (workerInfo.pendingBatches === 0) {
							console.log(`Worker ${workerId} is ready to receive batches`);
							logger.info(`Worker ${workerId} is ready to receive batches`);
						}
					} else if (data.status === 'done') {
						await state.updateWorkerActivity(workerId);
						console.log(`Worker ${workerId} is ready for new batch`);
						logger.info(`Worker ${workerId} is ready for new batch`);
					}
				} else if (topic === process.env.KAFKA_WORKER_SEND_TOPIC) {
					const { batchId, processing, total } = data;
					console.log(
						`${Date.now()} - Worker ${workerId} processing batch ${batchId}: ${processing}/${total}`
					);
					logger.info(
						`Worker ${workerId} processing batch ${batchId}: ${processing}/${total}`,
						Date.now()
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

	async sendHeartbeat(masterId) {
		if (!this.isProducerConnected) {
			await this.connect();
			console.log(`${masterId}: Producer connected for heartbeat`);
		}

		while (true) {
			try {
				await this.producer.send({
					topic: process.env.KAFKA_MASTER_HEARTBEAT_TOPIC,
					messages: [
						{ key: masterId, value: JSON.stringify({ status: 'alive' }) },
					],
				});
				logger.info(`${masterId}: Heartbeat sent`);
			} catch (err) {
				console.error(`${masterId}: Error sending heartbeat:`, err);
				logger.error(`${masterId}: Error sending heartbeat:`, err);
				this.isProducerConnected = false;
				await this.producer.connect();
				this.isProducerConnected = true;
			}
			await new Promise((resolve) => setTimeout(resolve, 5000)); // Heartbeat every 5 seconds
		}
	}

	async startLeaderElection(masterId, onBecomeLeader) {
		// Connect consumer and subscribe to heartbeat topic
		await this.consumer.subscribe({
			topic: process.env.KAFKA_MASTER_HEARTBEAT_TOPIC,
			fromBeginning: true,
		});

		// Connect producer
		await this.connect();

		let isLeader = false;
		let currentLeaderId = null;

		// Check for existing leader by consuming heartbeats
		console.log(`${masterId}: Starting leader election...`);
		await this.consumer.run({
			eachMessage: async ({ message }) => {
				const senderId = message.key.toString();
				if (senderId !== masterId) {
					currentLeaderId = senderId;
					console.log(
						`${masterId}: Detected heartbeat from ${currentLeaderId}`
					);
				}
			},
		});

		// Wait briefly to check for any existing heartbeats
		await new Promise((resolve) => setTimeout(resolve, 2000)); // 2-second initial check

		if (!currentLeaderId) {
			// No other master detected, this one becomes leader
			console.log(`${masterId}: No other masters detected, I am the leader!`);
			isLeader = true;
			// Start sending heartbeats
			this.sendHeartbeat(masterId).catch((err) => {
				console.error(`${masterId}: Heartbeat failed:`, err);
			});
			onBecomeLeader(masterId);
		} else {
			console.log(`${masterId}: ${currentLeaderId} is leader, I will standby.`);
		}

		// Monitor leader status for standby masters
		let lastHeartbeat = Date.now();
		const timeout = 10000; // 10-second timeout for leader failure

		while (true) {
			if (!isLeader) {
				await this.consumer.run({
					eachMessage: async ({ message }) => {
						const senderId = message.key.toString();
						if (senderId !== masterId) {
							lastHeartbeat = Date.now();
							currentLeaderId = senderId;
							logger.debug(
								`${masterId}: Received heartbeat from ${currentLeaderId}`
							);
						}
					},
				});

				if (Date.now() - lastHeartbeat > timeout && !isLeader) {
					console.log(
						`${masterId}: Leader ${currentLeaderId} failed, I am taking over!`
					);
					isLeader = true;
					currentLeaderId = masterId;
					this.sendHeartbeat(masterId).catch((err) => {
						console.error(`${masterId}: Heartbeat failed:`, err);
					});
					onBecomeLeader(masterId);
				}
			}
			await new Promise((resolve) => setTimeout(resolve, 1000)); // Check every second
		}
	}
}

export default KafkaManager;
