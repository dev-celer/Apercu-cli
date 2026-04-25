FROM ubuntu:latest
LABEL authors="s.montard"

COPY bin/apercu-pgproxy /
RUN chmod +x /apercu-pgproxy
EXPOSE 5432
ENTRYPOINT [ "/apercu-pgproxy" ]