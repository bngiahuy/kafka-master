FROM node:22-alpine

WORKDIR /app

RUN apk update && apk add --no-cache cifs-utils

COPY package*.json ./
RUN npm install

# mount samba
RUN echo -e '#!/bin/sh\n\
mkdir -p /mnt/samba && \\\n\
mount -t cifs -o username=${SMB_USER},password=${SMB_PASSWORD},port=${SMB_PORT},rw //${SMB_HOST}/share /mnt/samba && \\\n\
npm start' > /entrypoint.sh

RUN chmod +x /entrypoint.sh

COPY . .

EXPOSE ${API_SERVER_PORT}

ENTRYPOINT ["/entrypoint.sh"]
