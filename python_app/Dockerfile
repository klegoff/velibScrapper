# syntax=docker/dockerfile:1

FROM python:3.9.13

WORKDIR /app

COPY requirements.txt ./
COPY app.py ./

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "app.py"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
