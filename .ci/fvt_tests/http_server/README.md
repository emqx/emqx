## http_server


The http server for emqx functional validation testing

### Build


    $ rebar3 compile

### Getting Started

```
1> http_server:start().
Start http_server listener on 8080 successfully.
ok
2> http_server:stop().
ok
```

### APIS

+ GET `/counter`

  返回计数器的值

+ POST `/counter`

  计数器加一

