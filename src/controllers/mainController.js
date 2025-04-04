import redis from '../configs/redisConfig.js';
import {
	getPartitionsService,
	updatePartitionsService,
} from '../services/partitionsService.js';

export const updateNumBatches = async (req, res) => {
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

export const updatePartitions = async (req, res) => {
	try {
		const { value } = req.query;
		const result = await updatePartitionsService(value);
		res.status(200).send(result);
	} catch (error) {
		res.status(500).send('Error updating partitions in Redis.');
	}
};

export const getNumBatches = async (req, res) => {
	try {
		const numBatches = await redis.get('numBatches');
		if (!numBatches) {
			await redis.set('numBatches', 1000);
			res.status(200).send(1000); // Nếu không có numBatches, set lại 1000 và trả về 1000
		} else {
			res.status(200).send(numBatches);
		}
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
