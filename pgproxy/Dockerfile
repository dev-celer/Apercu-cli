FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags '-s -w' -o /apercu-pgproxy ./pgproxy

FROM ubuntu:latest
LABEL authors="s.montard"

RUN apt-get update -y && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /apercu-pgproxy /
EXPOSE 5432
ENTRYPOINT [ "/apercu-pgproxy" ]