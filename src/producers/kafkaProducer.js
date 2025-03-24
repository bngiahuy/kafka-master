import 'dotenv/config';
import kafka from '../configs/kafkaConfig.js';
import fs from 'fs';
import path from 'path';
import redis from '../configs/redisConfig.js';
import { randomUUID } from 'crypto';
const producer = kafka.producer();

const BATCH_DIR = './input_ip_data';
const BATCH_FILES = fs.readdirSync(BATCH_DIR).filter((f) => f.endsWith('.txt'));

let batchQueue = [...BATCH_FILES];

export const assignBatches = async () => {
	const assignTasks = [];
	const workers = await redis.hgetall('worker:status');

	for (const [workerId, status] of Object.entries(workers)) {
		if (status === '1' && batchQueue.length > 0) {
			const partitionRaw = await redis.hget('worker:partition', workerId);
			if (!partitionRaw) {
				console.warn(`⚠️ No partition assigned to ${workerId}, skipping!`);
				continue;
			}

			let partitions;
			try {
				partitions = JSON.parse(partitionRaw);
			} catch (err) {
				console.error(`❌ Error parsing partitions for ${workerId}:`, err);
				continue;
			}

			if (!Array.isArray(partitions) || partitions.length === 0) {
				console.warn(`⚠️ Invalid partition list for ${workerId}, skipping!`);
				continue;
			}

			const partition =
				partitions[Math.floor(Math.random() * partitions.length)];
			const nextBatch = batchQueue.shift();

			const task = runProducer(workerId, nextBatch, partition)
				.then(() => redis.hset('worker:status', workerId, '0'))
				.catch((err) =>
					console.error(`❌ Failed to assign batch to ${workerId}:`, err)
				);

			assignTasks.push(task);
		}
	}
	// Wait for all tasks to complete, to ensure all workers are assigned
	await Promise.all(assignTasks);
};
const runProducer = async (workerId, batchFileName, partition) => {
	await producer.connect();

	const batchId = path.basename(batchFileName, '.txt') || randomUUID(); // If no filename, generate a random UUID
	const ipList = fs
		.readFileSync(path.join(BATCH_DIR, batchFileName), 'utf-8')
		.split('\n')
		.filter((line) => line.trim());

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
