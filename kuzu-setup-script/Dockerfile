FROM alpine:3.22.2

WORKDIR /setup

COPY kuzu-data .

CMD ["sh", "-c", "cp -a ./* /data/kuzu"]