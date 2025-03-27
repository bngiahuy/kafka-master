import redis from '../configs/redisConfig.js';
import {
	getPartitionsService,
	getWorkersStatusService,
	updatePartitionsService,
} from '../services/mainService.js';
import 'dotenv/config';
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
		res.status(200).send(`numBatches updated to ${numBatches}.`);
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
		let numBatches = await redis.get('numBatches');
		if (!numBatches) {
			numBatches = process.env.BATCH_SIZE || 1000;
		}
		res.status(200).send(numBatches);
	} catch (error) {
		res.status(500).send('Error getting numBatches from Redis.');
	}
};

export const getWorkersStatus = async (req, res) => {
	try {
		const result = await getWorkersStatusService();
		res.status(200).send(result);
	} catch (error) {
		res.status(500).send('Error getting workers status from Redis.');
	}
};
