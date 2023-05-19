ARG CASSANDRA_TAG=3.11.6
FROM cassandra:${CASSANDRA_TAG}
COPY cassandra.yaml /etc/cassandra/cassandra.yaml
CMD ["cassandra", "-f"]
