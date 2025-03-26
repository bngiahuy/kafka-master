import redis from '../configs/redisConfig.js';
import getPartitionsService from '../services/getPartitionsService.js';

export const assignNumBatches = async (req, res) => {
	try {
		let { numBatches } = req.query;
		if (!numBatches || isNaN(numBatches) || parseInt(numBatches) < 1) {
			console.log(
				'⚠️ Invalid or missing numBatches query param, using default 1000.'
			);
			numBatches = 1000; // Gán giá trị mặc định
		} else {
			numBatches = parseInt(numBatches);
		}
		await redis.set('numBatches', numBatches);
		console.log(`🔧 Updated numBatches to ${numBatches}`);
		res
			.status(200)
			.send(`numBatches updated to ${numBatches}. Assigner loop continues.`);
	} catch (error) {
		console.error('❌ Error updating numBatches:', error);
		res.status(500).send('Error updating numBatches in Redis.');
	}
};

export const getPartitions = async (req, res) => {
	try {
		const partitions = await getPartitionsService();
		res.status(200).send(partitions);
	} catch (error) {
		res.status(500).send('Error getting partitions from Kafka.');
	}
};

export const getNumBatches = async (req, res) => {
	try {
		const numBatches = await redis.get('numBatches');
		res.status(200).send(numBatches);
	} catch (error) {
		res.status(500).send('Error getting numBatches from Redis.');
	}
};

export const getWorkersStatus = async (req, res) => {
	try {
		const workersStatus = await redis.hgetall('worker:status');
		res.status(200).send(workersStatus);
	} catch (error) {
		res.status(500).send('Error getting Workers status');
	}
};
