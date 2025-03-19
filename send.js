// server.js (Producer)
const amqp = require('amqplib');

async function sendToQueue() {
  const queue = 'hello'; // Tên queue
  const msg = 'Hello World!'; // Nội dung message

  try {
    // Tạo kết nối tới RabbitMQ
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    
    // Tạo queue nếu chưa có
    await channel.assertQueue(queue, { durable: false });

    // Gửi message vào queue
    channel.sendToQueue(queue, Buffer.from(msg));
    console.log(`[x] Sent ${msg}`);

    // Đóng kết nối
    setTimeout(() => {
      channel.close();
      connection.close();
    }, 500);
  } catch (error) {
    console.error(error);
  }
}

sendToQueue();

