import fs from 'fs';
const logMessage = (message) => {
	fs.appendFileSync(
		'nodeapp.log',
		`[${new Date().toISOString()}] - ${message}\n`,
		'utf-8'
	);
};

export default logMessage;
