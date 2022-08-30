# syntax=docker/dockerfile:1

FROM python:3.9.13

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY writeDB.py writeDB.py

CMD [ "python3", "writeDB.py"]
