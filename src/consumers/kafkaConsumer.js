import kafka from '../configs/kafkaConfig.js';

const consumer = kafka.consumer({ groupId: 'test-group' });

export const runConsumer = async () => {
	await consumer.connect();
	console.log('Consumer connected');

	await consumer.subscribe({
		topic: process.env.KAFKA_TOPIC_NAME,
		fromBeginning: true,
	});

	await consumer.run({
		eachMessage: async ({ message }) => {
			console.log(
				`Received message: ${message.value.toString()} with key ${
					message.key ? message.key.toString() : ''
				}`
			);
			await processMessage(message); // Process the message
		},
	});
};
