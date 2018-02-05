FROM ubuntu:xenial
RUN apt-get update && apt-get -y install curl netcat jq

COPY ./bin/linux/prometheus_adapter /prometheus_adapter
COPY ./conf/adapter_config.json /etc/storage_adapter/adapter_config.json

ENTRYPOINT ["/prometheus_adapter"]
