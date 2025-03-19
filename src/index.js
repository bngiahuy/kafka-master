import { runConsumer } from './consumers/kafkaConsumer.js';
import logMessage from './utils/logger.js';

const startApp = async () => {
	try {
		await runConsumer();
	} catch (error) {
		logMessage('Error occurred while running the consumer: ' + error.message);
	}
};

startApp();
