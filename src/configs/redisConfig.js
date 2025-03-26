import Redis from 'ioredis';

const redis = new Redis({
	host: 'redis', // tên container Redis
	port: 6379, // port mặc định của Redis
});

export default redis;
