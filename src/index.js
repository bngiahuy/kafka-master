import { runConsumer } from './consumers/kafkaConsumer.js';

const startApp = async () => {
	await runConsumer();
};

startApp();
