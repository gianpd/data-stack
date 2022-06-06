FROM python:3.10.4-slim-buster

WORKDIR /usr/src/app

# set environment variables
# Prevents Python from writing pyc files to disc
# Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE 1 
ENV PYTHONUNBUFFERED 1

# RUN curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o \
#         /usr/share/keyrings/redis-archive-keyring.gpg && \
#     echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] 
#         https://packages.redis.io/deb $(lsb_release -cs) main" \
#         | sudo tee /etc/apt/sources.list.d/redis.list && \
#     sudo apt-get update && sudo apt-get install redis

COPY docker/requirements.txt /app/requirements.txt
# install system dependencies
RUN apt-get update \
  && apt-get -y install netcat gcc\
  && apt-get clean

RUN python -m pip install --upgrade pip && \
    pip install python-multipart && \
    pip install -r /app/requirements.txt 

COPY ./app .
CMD bash bootstrap.sh

