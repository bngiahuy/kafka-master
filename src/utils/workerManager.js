import redis from '../configs/redisConfig.js';
import { acquireLock, releaseLock } from './helper.js';
import { logMessage } from './logger.js';

// Cơ chế xử lý định kỳ cập nhật worker
export const startWorkerUpdater = () => {
    console.log('🔄 Starting worker updater service...');

    const intervalId = setInterval(async () => {
        try {
            // Lấy danh sách worker cần cập nhật
            const workerKeys = await redis.keys('worker:needs:update:*');

            if (workerKeys.length > 0) {
                console.log(`🔍 Found ${workerKeys.length} workers pending updates`);
            }

            for (const key of workerKeys) {
                const workerId = key.split(':')[3]; // worker:needs:update:{workerId}

                // Kiểm tra thông tin cập nhật
                const pendingData = await redis.hgetall(`worker:pending:update:${workerId}`);
                if (!pendingData || Object.keys(pendingData).length === 0) {
                    console.log(`⚠️ No pending data for worker ${workerId}, removing flag`);
                    await redis.del(`worker:needs:update:${workerId}`);
                    continue;
                }

                // Kiểm tra xem worker có đang bị lock không
                const isLocked = await redis.exists(`lock:${workerId}`);
                if (isLocked) {
                    console.log(`⏳ Worker ${workerId} is currently locked, will retry later`);
                    continue;
                }

                // Cố gắng lấy lock để cập nhật
                const hasLock = await acquireLock(workerId, 3000);
                if (!hasLock) {
                    console.log(`⌛ Could not acquire lock for worker ${workerId}, will retry later`);
                    continue;
                }

                try {
                    console.log(`🔐 Acquired lock for worker ${workerId}, applying pending updates`);

                    // Áp dụng các cập nhật theo thứ tự ưu tiên
                    const { partitions, status, timestamp } = pendingData;
                    const updateTime = new Date(parseInt(timestamp, 10)).toISOString();

                    if (partitions) {
                        await redis.hset('worker:partition', workerId, partitions);
                        console.log(`✅ Updated partitions for worker ${workerId} (queued at ${updateTime})`);
                    }

                    if (status) {
                        if (status === 'new' || status === 'done') {
                            await redis.hset('worker:status', workerId, '1'); // Đặt trạng thái thành sẵn sàng
                            console.log(`✅ Updated status to ready (1) for worker ${workerId}`);
                        }
                    }

                    // Xóa dữ liệu cập nhật sau khi đã xử lý
                    await redis.del(`worker:pending:update:${workerId}`);
                    await redis.del(`worker:needs:update:${workerId}`);

                    logMessage(`Applied pending updates for worker ${workerId}`);
                } catch (err) {
                    console.error(`❌ Error applying updates for worker ${workerId}:`, err);
                } finally {
                    // Luôn giải phóng lock sau khi hoàn thành
                    await releaseLock(workerId);
                }
            }
        } catch (err) {
            console.error('❌ Error in worker updater service:', err);
        }
    }, 3000); // Chạy mỗi 3 giây

    return () => clearInterval(intervalId);
};

// Kiểm tra và xử lý worker đang timeout
export const checkAndCleanupWorker = async (workerId) => {
    try {
        // Kiểm tra xem worker có đang xử lý batch nào không
        const batchInfo = await redis.hget('worker:batchInfo', workerId);
        if (!batchInfo) return false; // Không có batch đang xử lý

        const parsedBatchInfo = JSON.parse(batchInfo);
        const { batchId, assignedAt } = parsedBatchInfo;

        // Kiểm tra thời gian xử lý
        const now = Date.now();
        const processingTime = now - assignedAt;
        const timeout = (process.env.WORKER_TIMEOUT || 60) * 1000;

        if (processingTime > timeout) {
            console.log(`🕒 Worker ${workerId} has been processing batch ${batchId} for too long (${processingTime / 1000}s)`);
            return true; // Worker đang timeout
        }

        return false; // Worker đang hoạt động bình thường
    } catch (err) {
        console.error(`❌ Error checking worker ${workerId} status:`, err);
        return false;
    }
}; 