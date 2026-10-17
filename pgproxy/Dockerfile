FROM ubuntu:latest
LABEL authors="s.montard"

RUN apt-get update -y && apt-get install -y ca-certificates
COPY bin/apercu-pgproxy /
RUN chmod +x /apercu-pgproxy
EXPOSE 5432
ENTRYPOINT [ "/apercu-pgproxy" ]