# syntax=docker/dockerfile:1

FROM python:3.9.13

WORKDIR /app

COPY requirements.txt ./
COPY requirements_front.txt ./
COPY front.py ./
COPY connection_utils.py ./

RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements_front.txt

EXPOSE 8050

CMD gunicorn --bind 0.0.0.0:8050 front:server
#ENTRYPOINT ["python", "front.py"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
