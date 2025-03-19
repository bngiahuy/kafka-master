// client.js
const { Kafka } = require('kafkajs');

// Tạo Kafka instance
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'] // Đảm bảo rằng Kafka đang chạy trên cổng này
});

// Tạo consumer
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  await consumer.connect();
  console.log('Consumer connected');

  // Đăng ký vào topic 'test-topic' và bắt đầu nhận message
  await consumer.subscribe({ topic: 'test-topic-1', fromBeginning: true });

  // Lắng nghe các thông điệp
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`);
    },
  });
};

run().catch(console.error);

