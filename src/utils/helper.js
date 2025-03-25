import redis from '../configs/redisConfig.js';

export const acquireLock = async (workerId, timeout = 1000) => {
	const lockKey = `lock:worker:${workerId}`;
	const startTime = Date.now();
	const result = await redis.set(lockKey, 'locked', 'NX', 'PX', timeout);
	const endTime = Date.now();
	console.log(
		`🚀 ~ acquireLock ~ worker: ${workerId}, result: ${result}, time: ${
			endTime - startTime
		}ms`
	);
	return result === 'OK';
};

export const releaseLock = async (workerId) => {
	const lockKey = `lock:worker:${workerId}`;
	await redis.del(lockKey);
};
