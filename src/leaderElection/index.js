import kafka from '../configs/kafkaConfig.js';
import 'dotenv/config';

async function sendHeartbeat(masterId) {
	const producer = kafka.producer();
	await producer.connect();
	while (true) {
		try {
			await producer.send({
				topic: process.env.KAFKA_MASTER_HEARTBEAT_TOPIC,
				messages: [
					{
						key: masterId,
						value: 'heartbeat',
						timestamp: Date.now().toString(),
					},
				],
			});
		} catch (error) {
			console.error(`${masterId}: Lỗi khi gửi heartbeat:`, error);
		}
		await new Promise((resolve) => setTimeout(resolve, 2000)); // Gửi heartbeat mỗi 2 giây
	}
}

export const startLeaderElection = async (masterId, onBecomeLeader) => {
	const consumer = kafka.consumer({ groupId: 'leader-election-group' });
	await consumer.connect();
	await consumer.subscribe({
		topic: process.env.KAFKA_MASTER_HEARTBEAT_TOPIC,
		fromBeginning: true,
	});

	// Bắt đầu gửi heartbeat
	sendHeartbeat(masterId).catch(console.error);

	console.log(`${masterId} khởi động, kiểm tra trạng thái...`);
	let hasOtherLeader = false;
	let isLeader = false;
	let lastHeartbeat = Date.now();

	// Vấn đề nằm ở phương thức chạy consumer
	// Thêm biến để đánh dấu đã hoàn thành quá trình khởi tạo
	let initializationComplete = false;

	consumer.run({
		eachMessage: async ({ message }) => {
			const messageKey = message.key.toString();
			const timestamp = Number(message.timestamp);
			if (messageKey !== masterId) {
				console.log(`${masterId} nhận heartbeat từ ${messageKey}, timestamp: ${new Date(timestamp).toISOString()}`);
				// Chỉ quan tâm heartbeat từ master khác
				hasOtherLeader = true;
				lastHeartbeat = Date.now();
			}
		},
	});

	// Chờ 5 giây để kiểm tra leader - Đảm bảo đã nhận đủ heartbeat
	console.log(`${masterId}: Đang chờ 5.5 giây để thu thập heartbeat...`);
	await new Promise((resolve) => setTimeout(resolve, 5500));

	// Sau khi chờ, đánh dấu quá trình khởi tạo hoàn tất
	initializationComplete = true;

	if (!hasOtherLeader) {
		isLeader = true;
		console.log(`${masterId}: Không thấy master khác, tôi là leader!`);
		onBecomeLeader(masterId);
	} else {
		console.log(`${masterId}: Đã có master khác làm leader, tôi sẽ standby.`);
	}

	// Theo dõi trạng thái liên tục
	const timeout = 10000; // 10 giây timeout

	while (true) {
		// Chỉ kiểm tra timeout khi đã hoàn thành khởi tạo và không phải leader
		if (initializationComplete && !isLeader && Date.now() - lastHeartbeat > timeout) {
			console.log(`${masterId}: Master khác đã chết (${Math.floor((Date.now() - lastHeartbeat) / 1000)}s không có heartbeat), tôi trở thành leader!`);
			isLeader = true;
			onBecomeLeader(masterId);
		}
		await new Promise((resolve) => setTimeout(resolve, 1000)); // Kiểm tra mỗi giây
	}
};

export default startLeaderElection;