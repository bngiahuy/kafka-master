import { Kafka } from 'kafkajs';
import 'dotenv/config';

const kafka = new Kafka({
	clientId: 'kafka-master',
	brokers: [
		`${process.env.KAFKA_BROKER_ADDRESS}:${process.env.KAFKA_BROKER_PORT}`,
	],
});

const admin = kafka.admin();
await admin
	.createTopics({
		topics: [
			{
				topic: process.env.KAFKA_TOPIC_NAME_MASTER,
				numPartitions: 10,
				replicationFactor: 1,
			},
			{
				topic: process.env.KAFKA_TOPIC_NAME_WORKER,
				numPartitions: 1,
				replicationFactor: 1,
			},
			{
				topic: process.env.KAFKA_TOPIC_NAME_WORKER_FREE,
				numPartitions: 1,
				replicationFactor: 1,
			},
		],
	})
	.then(async (result) => {
		if (result) {
			console.log('Topics created successfully!');
		} else {
			console.log('Topics have already existed!');
		}
		console.log(await admin.listTopics());
	})
	.catch((err) => {
		console.error('Failed to create topics: ', err);
	});

await admin.disconnect();

export default kafka;
