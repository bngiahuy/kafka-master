import runProducer, { assignBatches } from '../producers/kafkaProducer.js';

export const sendSignal = async (req, res) => {
	try {
		await assignBatches();
		res.status(200).send('Message sent to Workers');
	} catch (error) {
		res.status(500).send('Error sending message to Workers');
	}
};
