ARG BUILD_FROM=redis:5
FROM ${BUILD_FROM}
COPY redis.conf /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]
