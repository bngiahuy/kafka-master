import express from 'express';
import { runConsumer } from './consumers/kafkaConsumer.js';
import mainRouter from './routes/mainRoute.js';
import { rateLimit } from 'express-rate-limit';

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

const startApp = async () => {
	await runConsumer();
};
startApp();

app.use(express.json());
// app.use(apiRateLimitting);
app.use('/api', mainRouter);
app.listen(port, () => {
	console.log(`Server is running on port ${port}`);
});
