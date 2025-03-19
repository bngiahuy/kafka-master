import express from 'express';
import { runConsumer } from './consumers/kafkaConsumer.js';
import mainRouter from './routes/mainRoute.js';
const app = express();
const port = process.env.API_SERVER_PORT || 3001;

const startApp = async () => {
	await runConsumer();
};
startApp();
app.use(express.json());
app.use('/api', mainRouter);
app.listen(port, () => {
	console.log(`Server is running on port ${port}`);
});
