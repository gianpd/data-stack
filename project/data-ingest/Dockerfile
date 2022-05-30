FROM python:3.10.4-slim-buster

WORKDIR /usr/src/home

# set environment variables
# Prevents Python from writing pyc files to disc
# Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE 1 
ENV PYTHONUNBUFFERED 1

COPY . .
RUN python -m pip install --upgrade pip && \
    pip install python-multipart && \
    pip install -r requirements.txt 

CMD bash bootstrap.sh

