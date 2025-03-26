FROM python:3.9

WORKDIR /app

# Cài đặt cifs-utils để mount SMB
RUN apt-get update && apt-get install -y cifs-utils

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Tạo script mount
RUN echo '#!/bin/bash\n\
mkdir -p /mnt/samba && \
mount -t cifs -o username=${SMB_USER},password=${SMB_PASS},port=${SMB_PORT},rw //${SMB_HOST}/share /mnt/samba && \
python worker.py' > /mount_and_run.sh

RUN chmod +x /mount_and_run.sh

ENTRYPOINT ["/mount_and_run.sh"] 