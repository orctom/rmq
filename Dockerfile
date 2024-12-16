FROM golang:1.23.1 AS build

ARG build_timestamp
ENV build_timestamp=$build_timestamp

EXPOSE 9001
EXPOSE 9002
CMD ["rmq"]
