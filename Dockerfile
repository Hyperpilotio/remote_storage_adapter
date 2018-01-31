FROM alpine:3.4

COPY remote_storage_adapter /remote_storage_adapter
COPY ./conf/adapter_config.json /etc/storage_adapter/adapter_config.json

ENTRYPOINT ["/remote_storage_adapter"]
