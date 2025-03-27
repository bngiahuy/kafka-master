import 'dotenv/config';
import State from './state.js';
import express from 'express';
import KafkaManager from './configs/kafkaConfig.js';
import { getIpFiles, readIpFile } from './utils/helper.js';
import redis from './configs/redisConfig.js';
import path from 'path';
import mainRouter from './routes/mainRoutes.js';
import startLeaderElection from './leaderElection/index.js';
import { randomUUID } from 'crypto';

async function processIpFile(filePath, kafkaManager, state) {
	const filename = path.basename(filePath);
	const processedFiles = await redis.smembers(state.processedFilesKey);
	if (processedFiles.includes(filename)) {
		console.log(`File ${filename} already processed, skipping`);
		await state.markFileProcessed(filename);
		return true;
	}

	const ipList = readIpFile(filePath);
	if (!ipList.length) {
		console.log(`File ${filename} is empty or unreadable`);
		await state.markFileProcessed(filename);
		return true;
	}

	const batches = await state.getBatchesForFile(filename);
	let startPos = 0;
	if (batches.length) {
		batches.sort((a, b) => a.startPos - b.startPos);
		for (const batch of batches) {
			if (startPos < batch.startPos) break;
			startPos = Math.max(startPos, batch.endPos);
		}
	}

	if (startPos >= ipList.length) {
		console.log(`File ${filename} fully processed`);
		await state.markFileProcessed(filename);
		return true;
	}

	const availableWorkers = await state.getAvailableWorkers();
	if (!availableWorkers.length) {
		console.log('No available workers');
		return false;
	}

	const workerId = availableWorkers[0];
	const batchSize =
		parseInt(await redis.get('numBatches')) ||
		parseInt(process.env.BATCH_SIZE) ||
		1000;

	const endPos = Math.min(startPos + batchSize, ipList.length);
	const ipBatch = ipList.slice(startPos, endPos);

	await kafkaManager.sendBatch(
		workerId,
		ipBatch,
		filename,
		startPos,
		endPos,
		state
	);

	if (endPos >= ipList.length) {
		await state.markFileProcessed(filename);
		console.log(`File ${filename} fully processed`);
		return true;
	}
	return false;
}

async function main(folderPath) {
	console.log(`Starting master process at ${new Date().toISOString()}`);
	console.log(`Processing IP files from ${folderPath}`);

	const state = new State();
	const kafkaManager = new KafkaManager();

	try {
		await kafkaManager.connect();

		const ipFiles = getIpFiles(folderPath);
		if (!ipFiles.length) {
			console.log(`No IP files found in ${folderPath}`);
			return;
		}
		await kafkaManager.runConsumer(state);

		console.log(`Found ${ipFiles.length} IP files to process`);

		while (true) {
			const inactiveWorkers = await state.removeInactiveWorkers(
				process.env.WORKER_TIMEOUT
			);
			if (inactiveWorkers.length) {
				console.log(`Removed inactive workers: ${inactiveWorkers}`);
			}

			const generalState = await state.loadGeneralState();
			const fileIndex = generalState.currentFileIndex;

			if (fileIndex < ipFiles.length) {
				const processed = await processIpFile(
					ipFiles[fileIndex],
					kafkaManager,
					state
				);
				if (processed) {
					generalState.currentFileIndex += 1;
					await state.saveGeneralState(generalState);
				}
			} else {
				const hasActiveBatches = await state.hasActiveBatches();
				if (!hasActiveBatches) {
					console.log('All files processed and no active batches. Exiting.');
					break;
				}
			}

			await new Promise((resolve) => setTimeout(resolve, 100));
		}
	} catch (e) {
		console.error(`Unexpected error: ${e}`);
	} finally {
		await kafkaManager.disconnect();
		console.log(`Master process completed at ${new Date().toISOString()}`);
	}
}

const app = express();
const port = process.env.API_SERVER_PORT || 3001;
const masterId = 'master-' + randomUUID();
const startApp = async (masterId) => {
	const onBecomeLeader = async (leaderId) => {
		console.log(
			`${leaderId}: I am now the leader, starting main process and API...`
		);
		main(process.argv[2]).catch((e) => console.error(e));
		app.use(express.json());
		app.use('/api', mainRouter);
		app.listen(port, () => {
			console.log(`Server ${leaderId} is running on port ${port}`);
		});
	};

	await startLeaderElection(masterId, onBecomeLeader);

	console.log(`${masterId}: In standby mode, waiting for leadership...`);
};

startApp(masterId);
