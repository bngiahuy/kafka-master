import express from 'express';
import { runConsumer } from './consumers/kafkaConsumer.js';
import mainRouter from './routes/mainRoute.js';
import { rateLimit } from 'express-rate-limit';
import { startBatchAssigner } from './producers/kafkaProducer.js';
import { startLeaderElection } from './leaderElection/index.js';
import { randomUUID } from 'crypto';
import 'dotenv/config';
const app = express();
const port = process.env.API_SERVER_PORT || 3001;

const apiRateLimitting = rateLimit({
	windowMs: 15 * 60 * 1000,
	standardHeaders: true,
	max: 100,
	handler: (req, res) => {
		res.status(429).send({
			status: 429,
			message: 'Too many requests! Please try again later.',
		});
	},
});

const startApp = async (masterId) => {

	// Callback khi trở thành leader
	const onBecomeLeader = async (leaderId) => {
		console.log(`${leaderId} bắt đầu chạy consumer và API...`);
		await runConsumer(); // Chạy consumer khi là leader
		await startBatchAssigner();
		app.use(express.json());
		// app.use(apiRateLimitting);
		app.use('/api', mainRouter); // API chỉ hoạt động khi là leader
		app.listen(port, () => {
			console.log(`Server ${leaderId} is running on port ${port}`);
		});
	};

	// Bắt đầu leader election mà không cần biết otherMasterId
	await startLeaderElection(masterId, onBecomeLeader);
};
const masterId = process.env.MASTER_ID || 'master-' + randomUUID();
startApp(masterId);
