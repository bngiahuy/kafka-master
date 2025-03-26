FROM node:22-alpine

WORKDIR /app

# Cài đặt cifs-utils để hỗ trợ mount SMB (Alpine dùng apk thay vì apt-get)
RUN apk update && apk add --no-cache cifs-utils

COPY package*.json ./
RUN npm install

COPY . .

# Tạo script để mount SMB và chạy ứng dụng
RUN echo -e '#!/bin/sh\n\
mkdir -p /mnt/samba && \\\n\
mount -t cifs -o username=${SMB_USER},password=${SMB_PASS},port=${SMB_PORT},rw //${SMB_HOST}/share /mnt/samba && \\\n\
npm start' > /mount_and_run.sh

RUN chmod +x /mount_and_run.sh

EXPOSE ${API_SERVER_PORT}
ENTRYPOINT ["/mount_and_run.sh"]
