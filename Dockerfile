FROM ubuntu:latest
LABEL authors="marton.aron"

COPY . .

RUN [""]

ENTRYPOINT ["python3", "run_on_server.py"]