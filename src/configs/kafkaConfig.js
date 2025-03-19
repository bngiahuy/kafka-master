import { Kafka } from 'kafkajs';
import 'dotenv/config';

const kafka = new Kafka({
	clientId: 'kafka-master',
	brokers: [
		`${process.env.KAFKA_BROKER_ADDRESS}:${process.env.KAFKA_BROKER_PORT}`,
	],
});

export default kafka;
