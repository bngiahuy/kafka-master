import kafka from '../configs/kafkaConfig.js';
import 'dotenv/config';
async function sendHeartbeat(masterId) {
	const producer = kafka.producer();
	await producer.connect();
	while (true) {
		await producer.send({
			topic: 'master-heartbeat',
			messages: [{ key: masterId, value: JSON.stringify({ status: 'alive' }) }],
		});
		await new Promise((resolve) => setTimeout(resolve, 1000)); // Gửi heartbeat mỗi 1 giây
	}
}

export const startLeaderElection = async (masterId, onBecomeLeader) => {
	const consumer = kafka.consumer({ groupId: 'leader-election-group' });
	await consumer.subscribe({
		topic: 'master-heartbeat',
		fromBeginning: true,
	});

	// Bắt đầu gửi heartbeat trước
	sendHeartbeat(masterId).catch(console.error);

	let hasOtherLeader = false;
	let isLeader = false;
	let lastHeartbeat = Date.now();
	const timeout = 10000; // 10 giây

	// **Chỉ run() 1 lần**: gom hết logic eachMessage vào đây
	consumer.run({
		eachMessage: async ({ message }) => {
			const otherMasterId = message.key.toString();
			if (otherMasterId !== masterId) {
				hasOtherLeader = true;
				lastHeartbeat = Date.now();
			}
		},
	});

	console.log(`${masterId} khởi động, kiểm tra trạng thái...`);

	// Chờ 5 giây để xem có master khác không
	await new Promise((resolve) => setTimeout(resolve, 5000));
	if (!hasOtherLeader) {
		console.log(`${masterId}: Không thấy master khác, tôi là leader!`);
		isLeader = true;
		onBecomeLeader(masterId);
	} else {
		console.log(`${masterId}: Đã có master khác làm leader, tôi sẽ standby.`);
	}

	// Tiếp tục theo dõi heartbeat
	while (true) {
		// Nếu mình chưa là leader mà quá 10 giây không thấy heartbeat => giành leader
		if (!isLeader && Date.now() - lastHeartbeat > timeout) {
			console.log(`${masterId}: Master khác đã chết, tôi trở thành leader!`);
			isLeader = true;
			onBecomeLeader(masterId);
		}
		await new Promise((resolve) => setTimeout(resolve, 1000));
	}

};
export default startLeaderElection;