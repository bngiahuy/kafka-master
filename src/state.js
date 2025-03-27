import Redis from 'ioredis';
import redis from './configs/redisConfig.js';
import logger from './utils/logger.js';
class State {
	constructor() {
		this.generalKey = 'state:general';
		this.processedFilesKey = 'processedFiles';
		this.workersPrefix = 'worker:';
		this.batchesPrefix = 'batch:';
		this.activeWorkersKey = 'activeWorkers';
	}

	// Load trạng thái chung
	async loadGeneralState() {
		const state = await redis.hgetall(this.generalKey);
		return {
			currentFileIndex: parseInt(state.currentFileIndex || '0'),
		};
	}

	// Lưu trạng thái chung
	async saveGeneralState(state) {
		await redis.hmset(this.generalKey, {
			currentFileIndex: state.currentFileIndex.toString(),
		});
	}

	// Thêm worker mới
	async addWorker(workerId, partitions) {
		const workerKey = `${this.workersPrefix}${workerId}`;
		await redis.hset(workerKey, {
			lastActivity: Date.now().toString(),
			partitions: JSON.stringify(partitions),
			pendingBatches: '0',
		});
		await redis.sadd(this.activeWorkersKey, workerId);
	}

	// Cập nhật thời gian hoạt động của worker
	async updateWorkerActivity(workerId) {
		const workerKey = `${this.workersPrefix}${workerId}`;
		await redis.hset(workerKey, 'lastActivity', Date.now().toString());
	}

	// Xóa worker không hoạt động
	async removeInactiveWorkers(timeout) {
		const now = Date.now();
		const workerIds = await redis.smembers(this.activeWorkersKey);
		const inactiveWorkers = [];

		for (const workerId of workerIds) {
			const workerKey = `${this.workersPrefix}${workerId}`;
			const lastActivity = parseInt(
				await redis.hget(workerKey, 'lastActivity')
			);
			if (now - lastActivity > timeout * 1000) {
				console.log(`Worker ${workerId} is offline! Deleting...`);
				logger.info(`Worker ${workerId} is offline! Deleting...`);
				inactiveWorkers.push(workerId);
				await redis.srem(this.activeWorkersKey, workerId);
				await redis.del(workerKey);

				// Đánh dấu các batch của worker này là thất bại
				const batchKeys = await redis.keys(`${this.batchesPrefix}*`);
				for (const batchKey of batchKeys) {
					const batch = await redis.hgetall(batchKey);
					if (batch.workerId === workerId && batch.status === 'doing') {
						await this.updateBatchStatus(batchKey.split(':')[1], 'fail');
					}
				}
			}
		}
		return inactiveWorkers;
	}

	// Lấy danh sách worker có sẵn
	async getAvailableWorkers() {
		const workerIds = await redis.smembers(this.activeWorkersKey);
		const availableWorkers = [];
		for (const workerId of workerIds) {
			const workerKey = `${this.workersPrefix}${workerId}`;
			const pendingBatches = parseInt(
				await redis.hget(workerKey, 'pendingBatches')
			);
			if (pendingBatches === 0) {
				availableWorkers.push(workerId);
			}
		}
		return availableWorkers;
	}

	// Lưu thông tin batch
	async saveBatch(batchId, file, startPos, endPos, status, workerId) {
		const batchKey = `${this.batchesPrefix}${batchId}`;
		await redis.hmset(batchKey, {
			file,
			startPos: startPos.toString(),
			endPos: endPos.toString(),
			status,
			workerId,
		});
		if (status === 'doing') {
			const workerKey = `${this.workersPrefix}${workerId}`;
			await redis.hincrby(workerKey, 'pendingBatches', 1);
		}
	}

	// Cập nhật trạng thái batch
	async updateBatchStatus(batchId, status) {
		const batchKey = `${this.batchesPrefix}${batchId}`;
		const batch = await redis.hgetall(batchKey);
		if (batch) {
			await redis.hset(batchKey, 'status', status);
			if (status !== 'doing') {
				const workerKey = `${this.workersPrefix}${batch.workerId}`;
				await redis.hincrby(workerKey, 'pendingBatches', -1);
			}
		}
	}

	// Kiểm tra batch đang hoạt động
	async hasActiveBatches() {
		const batchKeys = await redis.keys(`${this.batchesPrefix}*`);
		for (const batchKey of batchKeys) {
			const batch = await redis.hgetall(batchKey);
			if (batch.status === 'doing') return true;
		}
		return false;
	}

	// Đánh dấu tệp đã xử lý
	async markFileProcessed(filename) {
		await redis.sadd(this.processedFilesKey, filename);
	}

	// Lấy thông tin worker
	async getWorkerInfo(workerId) {
		const workerKey = `${this.workersPrefix}${workerId}`;
		const workerData = await redis.hgetall(workerKey);
		return {
			lastActivity: parseInt(workerData.lastActivity),
			partitions: JSON.parse(workerData.partitions || '[]'),
			pendingBatches: parseInt(workerData.pendingBatches || '0'),
		};
	}

	// Lấy tất cả batch của tệp
	async getBatchesForFile(filename) {
		const batchKeys = await redis.keys(`${this.batchesPrefix}*`);
		const batches = [];
		for (const batchKey of batchKeys) {
			const batch = await redis.hgetall(batchKey);
			if (batch.file === filename) {
				batches.push({
					batchId: batchKey.split(':')[1],
					startPos: parseInt(batch.startPos),
					endPos: parseInt(batch.endPos),
					status: batch.status,
					workerId: batch.workerId,
				});
			}
		}
		return batches;
	}

	// Lấy toàn bộ worker và trạng thái của chúng (doing/free)
	async getAllWorkersStatus() {
		const workerIds = await redis.smembers(this.activeWorkersKey);
		const workersStatus = {};

		for (const workerId of workerIds) {
			const workerKey = `${this.workersPrefix}${workerId}`;
			const workerData = await redis.hgetall(workerKey);
			const pendingBatches = parseInt(workerData.pendingBatches || '0');
			const status = pendingBatches > 0 ? 'doing' : 'free';

			workersStatus[workerId] = {
				status,
				lastActivity: parseInt(workerData.lastActivity),
				partitions: JSON.parse(workerData.partitions || '[]'),
				pendingBatches,
			};
		}

		return workersStatus;
	}
}

export default State;
