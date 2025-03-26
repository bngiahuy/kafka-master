FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE ${API_SERVER_PORT}
CMD ["npm", "start"]
