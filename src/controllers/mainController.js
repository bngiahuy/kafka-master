import redis from '../configs/redisConfig.js';
import { assignBatches } from '../producers/kafkaProducer.js';

export const sendSignal = async (req, res) => {
	try {
		// Get parameters from request. this is a GET request
		const { numBatches } = req.query;
		if (!numBatches || isNaN(numBatches) || numBatches < 1) {
			// Set default value for numBatches if it is not provided
			numBatches = 1000;
		}
		await redis.set('numBatches', numBatches);
		await assignBatches();
		res.status(200).send('Message sent to Workers');
	} catch (error) {
		res.status(500).send('Error sending message to Workers');
	}
};
