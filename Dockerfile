FROM

ARG build_timestamp
ENV build_timestamp=$build_timestamp

EXPOSE 9001
EXPOSE 9002
ENTRYPOINT ["./bootstrap.sh"]
CMD ["app"]
