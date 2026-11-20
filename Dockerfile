FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    BLOCK_SIZE=4096

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY app /srv/app

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 单实例：只起一个 worker，进程内锁与 pins 目录均依赖单进程语义。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
