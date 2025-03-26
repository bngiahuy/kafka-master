import kafka from '../configs/kafkaConfig.js';

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'master-group' });

async function sendHeartbeat(masterId) {
    await producer.connect();
    while (true) {
        await producer.send({
            topic: process.env.KAFKA_TOPIC_NAME_HEARTBEAT,
            messages: [{ key: masterId, value: JSON.stringify({ status: 'alive' }) }],
        });
        await new Promise(resolve => setTimeout(resolve, 5000)); // Gửi heartbeat mỗi 5 giây
    }
}

export const startLeaderElection = async (masterId, onBecomeLeader) => {
	await consumer.connect();
    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC_NAME_HEARTBEAT, fromBeginning: true });

	// Bắt đầu gửi heartbeat
    sendHeartbeat(masterId).catch(console.error);

	console.log(`${masterId} khởi động, kiểm tra trạng thái...`);
    let hasOtherLeader = false;
    let isLeader = false;

    consumer.run({
        eachMessage: async ({ message }) => {
            if (message.key.toString() !== masterId) {
                hasOtherLeader = true;
            }
        },
    });

    // Chờ 5 giây để kiểm tra leader
    await new Promise(resolve => setTimeout(resolve, 5000));

    if (!hasOtherLeader) {
        console.log(`${masterId}: Không thấy master khác, tôi là leader!`);
        isLeader = true;
        onBecomeLeader(masterId);
    } else {
        console.log(`${masterId}: Đã có master khác làm leader, tôi sẽ standby.`);
    }

    // Theo dõi trạng thái liên tục
    let lastHeartbeat = Date.now();
    const timeout = 10000; // 10 giây timeout

    consumer.run({
        eachMessage: async ({ message }) => {
            if (message.key.toString() !== masterId) { // Chỉ quan tâm heartbeat từ master khác
                lastHeartbeat = Date.now();
            }
        },
    });

    while (true) {
        if (!isLeader && Date.now() - lastHeartbeat > timeout) {
            console.log(`${masterId}: Master khác đã chết, tôi trở thành leader!`);
            isLeader = true;
            onBecomeLeader(masterId);
        }
        await new Promise(resolve => setTimeout(resolve, 1000)); // Kiểm tra mỗi giây
    }
};