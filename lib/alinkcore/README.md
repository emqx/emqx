# alinkcore
core for iot

## install mysql
```shell
docker run -itd --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.7
```

## install redis
```shell
docker run -p 6379:6379 --name redis \
  -v /mydata/redis/data:/data \
  -v /mydata/redis/conf/redis.conf:/etc/redis/redis.conf \
  --restart=always \
  --network common-network \
  -d redis redis-server /etc/redis/redis.conf

```

## build
```bash
git clone --branch alinkcore git@github.com:alinkiot/emqx.git
BUILD_WITHOUT_QUIC=1 make
```
