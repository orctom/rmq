FROM golang:1.23.1 AS build

WORKDIR /app
RUN apt-get update && apt-get install -y upx
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build

FROM scratch
COPY --from=builder /app/rmq-server /rmq-server
COPY --from=builder /app/rmq-cli /rmq-cli

ARG build_timestamp
ENV build_timestamp=$build_timestamp

EXPOSE 9001
EXPOSE 9002
ENTRYPOINT ["rmq-server"]
