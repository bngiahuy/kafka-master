import kafka from '../configs/kafkaConfig.js';

const producer = kafka.producer();

const runProducer = async () => {
	await producer.connect();
	console.log('Producer connected');

	// Send message to topic
	try {
		await producer.send({
			topic: process.env.KAFKA_TOPIC_NAME_MASTER,
			messages: [{ value: 'Signal from master' }],
		});
	} catch (error) {
		throw error;
	}

	console.log('Message sent');
	await producer.disconnect();
};

export default runProducer;
