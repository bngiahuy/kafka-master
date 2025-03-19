import kafka from '../configs/kafkaConfig.js';

const consumer = kafka.consumer({ groupId: 'master-VM' });

export const runConsumer = async () => {
	await consumer.connect();
	console.log('Consumer connected');

	await consumer.subscribe({
		topic: process.env.KAFKA_TOPIC_NAME_WORKER,
		fromBeginning: true,
	});

	await consumer.run({
		eachMessage: async ({ message }) => {
			console.log(
				`Received message: ${message.value.toString()} with key ${
					message.key ? message.key.toString() : ''
				}`
			);
		},
	});
};
