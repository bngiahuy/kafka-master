import runProducer from '../producers/kafkaProducer.js';

export const sendSignal = async (req, res) => {
	try {
		await runProducer();
		res.status(200).send('Message sent to Workers');
	} catch (error) {
		res.status(500).send('Error sending message to Workers');
	}
};
