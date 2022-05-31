FROM python:3.10.4-slim-buster

WORKDIR /usr/src/app

# set environment variables
# Prevents Python from writing pyc files to disc
# Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE 1 
ENV PYTHONUNBUFFERED 1

COPY . .

# RUN curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o \
#         /usr/share/keyrings/redis-archive-keyring.gpg && \
#     echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] 
#         https://packages.redis.io/deb $(lsb_release -cs) main" \
#         | sudo tee /etc/apt/sources.list.d/redis.list && \
#     sudo apt-get update && sudo apt-get install redis

RUN python -m pip install --upgrade pip && \
    pip install python-multipart && \
    pip install -r requirements.txt 


CMD bash bootstrap.sh

