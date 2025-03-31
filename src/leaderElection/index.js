import redis from '../configs/redisConfig.js';
import 'dotenv/config';
import { setTimeout } from 'timers/promises';

// Các key Redis
const MASTERS_LIST_KEY = 'masters:list';  // Sorted set chứa các master
const MASTER_HEARTBEAT_KEY = 'master:heartbeat:';  // Key riêng cho heartbeat
const ACTIVE_LEADER_KEY = 'master:active:leader';  // Chỉ định rõ ai là leader đang hoạt động
const ELECTION_RUNNING_KEY = 'master:election:running'; // Đánh dấu đang có election
const MASTER_HEARTBEAT_INTERVAL = 3000; // 3 giây
const MASTER_TIMEOUT = 15000; // 15 giây không có heartbeat = die
const ELECTION_TIMEOUT = 10000; // Election tối đa 10 giây

// Gửi heartbeat định kỳ cho master hiện tại
const sendHeartbeat = async (masterId) => {
	console.log(`💓 Gửi heartbeat cho ${masterId}`);

	const intervalId = setInterval(async () => {
		try {
			// Cập nhật heartbeat với timestamp hiện tại
			await redis.set(`${MASTER_HEARTBEAT_KEY}${masterId}`, Date.now(), 'PX', MASTER_TIMEOUT * 2);

			// Cập nhật cả active leader key nếu master này là leader
			const currentLeader = await redis.get(ACTIVE_LEADER_KEY);
			if (currentLeader === masterId) {
				await redis.set(ACTIVE_LEADER_KEY, masterId, 'PX', MASTER_TIMEOUT * 2);
			}
		} catch (err) {
			console.error(`❌ Lỗi khi gửi heartbeat cho ${masterId}:`, err);
		}
	}, MASTER_HEARTBEAT_INTERVAL);

	return () => clearInterval(intervalId);
};

export const startLeaderElection = async (masterId, onBecomeLeader) => {
	console.log(`🚀 ${masterId} khởi động, bắt đầu quá trình election...`);
	let isLeader = false;
	let stopHeartbeat = null;
	let stopPolling = null;
	let leaderActivated = false; // Đánh dấu đã kích hoạt leader callback

	try {
		// Bước 1: Đăng ký master vào hệ thống
		// Sử dụng sorted set với thời gian làm score để biết master nào join trước
		await redis.zadd(MASTERS_LIST_KEY, Date.now(), masterId);

		// Bước 2: Thêm heartbeat ban đầu
		await redis.set(`${MASTER_HEARTBEAT_KEY}${masterId}`, Date.now(), 'PX', MASTER_TIMEOUT * 2);

		// Bắt đầu gửi heartbeat ngay lập tức
		stopHeartbeat = await sendHeartbeat(masterId);

		// Bước 3: Kiểm tra có leader hiện tại không
		const currentLeader = await redis.get(ACTIVE_LEADER_KEY);

		if (!currentLeader) {
			// Không có leader, tiến hành chọn leader đầu tiên
			console.log(`🔍 Không tìm thấy leader hiện tại, tiến hành chọn leader đầu tiên...`);
			await initiateElection();
		} else {
			console.log(`👑 Leader hiện tại: ${currentLeader}`);
			// Nếu master này là leader (trường hợp khởi động lại)
			if (currentLeader === masterId) {
				await becomeLeader();
			}
		}

		// Bước 4: Khởi động polling để kiểm tra leader
		stopPolling = startLeaderPolling();

		// Bước 5: Đảm bảo dọn dẹp khi container dừng
		process.on('SIGTERM', cleanup);
		process.on('SIGINT', cleanup);

	} catch (err) {
		console.error(`❌ Lỗi trong quá trình khởi tạo leader election:`, err);
	}

	// Hàm tiến hành bầu cử
	async function initiateElection() {
		// Kiểm tra xem đã có cuộc bầu cử nào đang diễn ra không
		const electionRunning = await redis.set(ELECTION_RUNNING_KEY, masterId, 'NX', 'PX', ELECTION_TIMEOUT);

		if (electionRunning !== 'OK') {
			console.log(`⏳ Đã có quá trình bầu cử đang diễn ra, chờ kết quả...`);
			return;
		}

		try {
			console.log(`🗳️ ${masterId} khởi xướng quá trình bầu cử...`);

			// Lấy danh sách master còn sống (có heartbeat)
			const allMasters = await redis.zrange(MASTERS_LIST_KEY, 0, -1);
			const liveMasters = [];

			for (const master of allMasters) {
				const heartbeat = await redis.get(`${MASTER_HEARTBEAT_KEY}${master}`);
				if (heartbeat) {
					liveMasters.push(master);
				} else {
					// Xóa master đã chết
					await redis.zrem(MASTERS_LIST_KEY, master);
					console.log(`🗑️ Xóa master không hoạt động: ${master}`);
				}
			}

			// Không có master nào còn sống
			if (liveMasters.length === 0) {
				console.log(`❌ Không có master nào còn sống, không thể bầu leader`);
				return;
			}

			// Chọn master đầu tiên (điểm thấp nhất) làm leader
			const newLeader = liveMasters[0];
			console.log(`👑 Chọn ${newLeader} làm leader mới`);

			// Đặt leader mới vào Redis với TTL
			await redis.set(ACTIVE_LEADER_KEY, newLeader, 'PX', MASTER_TIMEOUT * 2);

			// Nếu master này là leader, kích hoạt
			if (newLeader === masterId) {
				await becomeLeader();
			}
		} finally {
			// Luôn đảm bảo xóa khóa election running
			await redis.del(ELECTION_RUNNING_KEY);
		}
	}

	// Xử lý khi master này trở thành leader
	async function becomeLeader() {
		if (!isLeader) {
			console.log(`👑 ${masterId} trở thành leader!`);
			isLeader = true;

			// Chỉ kích hoạt callback một lần
			if (!leaderActivated) {
				leaderActivated = true;
				onBecomeLeader(masterId);
			}
		}
	}

	// Kiểm tra trạng thái leader hiện tại
	async function checkLeaderStatus() {
		try {
			// Lấy thông tin leader hiện tại
			const currentLeader = await redis.get(ACTIVE_LEADER_KEY);

			// Nếu master này là leader
			if (currentLeader === masterId) {
				await becomeLeader();
				return;
			}

			// Reset trạng thái nếu không còn là leader
			if (isLeader) {
				console.log(`⬇️ ${masterId} không còn là leader.`);
				isLeader = false;
			}

			// Nếu không có leader hoặc leader không còn heartbeat, bắt đầu bầu cử
			if (!currentLeader) {
				console.log(`⚠️ Không có leader hiện tại, cần bầu leader mới`);
				await initiateElection();
				return;
			}

			// Kiểm tra heartbeat của leader hiện tại
			const leaderHeartbeat = await redis.get(`${MASTER_HEARTBEAT_KEY}${currentLeader}`);
			if (!leaderHeartbeat) {
				console.log(`⚠️ Leader ${currentLeader} không có heartbeat, cần bầu leader mới`);

				// Xóa leader không còn hoạt động
				await redis.del(ACTIVE_LEADER_KEY);
				await redis.zrem(MASTERS_LIST_KEY, currentLeader);

				// Tiến hành bầu cử mới
				await initiateElection();
			} else {
				console.log(`🔍 ${masterId} đang standby. Leader hiện tại: ${currentLeader}`);
			}
		} catch (err) {
			console.error(`❌ Lỗi khi kiểm tra trạng thái leader:`, err);
		}
	}

	// Bắt đầu polling định kỳ để kiểm tra leader
	function startLeaderPolling() {
		console.log(`🔄 ${masterId} bắt đầu polling để kiểm tra leader`);

		const pollInterval = setInterval(async () => {
			try {
				await checkLeaderStatus();
			} catch (err) {
				console.error(`❌ Lỗi trong quá trình polling:`, err);
			}
		}, 5000); // Poll mỗi 5 giây

		return () => clearInterval(pollInterval);
	}

	// Hàm dọn dẹp khi container dừng
	async function cleanup() {
		console.log(`🧹 ${masterId} đang dọn dẹp trước khi dừng...`);

		try {
			// Dừng heartbeat và polling
			if (stopHeartbeat) {
				stopHeartbeat();
			}

			if (stopPolling) {
				stopPolling();
			}

			// Xóa master khỏi danh sách
			await redis.zrem(MASTERS_LIST_KEY, masterId);
			await redis.del(`${MASTER_HEARTBEAT_KEY}${masterId}`);

			// Nếu master này là leader, xóa khỏi active leader để master khác tiếp quản
			const currentLeader = await redis.get(ACTIVE_LEADER_KEY);
			if (currentLeader === masterId) {
				await redis.del(ACTIVE_LEADER_KEY);
				console.log(`👑 ${masterId} đã từ bỏ vai trò leader`);
			}

			console.log(`👋 ${masterId} đã dọn dẹp xong.`);
		} catch (err) {
			console.error(`❌ Lỗi khi dọn dẹp:`, err);
		}

		// Thoát quá trình nếu được gọi từ handler
		if (process.listenerCount('SIGINT') > 0 || process.listenerCount('SIGTERM') > 0) {
			process.exit(0);
		}
	}

	// Trả về hàm cleanup để có thể gọi thủ công nếu cần
	return cleanup;
};

export default startLeaderElection;