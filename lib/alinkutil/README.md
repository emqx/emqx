# alinkutil
this is an erlang util lib

erlang工具库

里面有一些工具类的模块+一些使用的小工具

## alg
算法库


## compiler
### modules
* alinkutil_dynamic_compile
* alinkutil_config_compiler
### alinkutil_dynamic_compile
将字符串动态编译成代码
```
2> alinkutil_dynamic_compile:load_from_string("-module(ff). -export([a/0]). a() -> a. \n").
{module,ff}
3> ff:a().
a
```
### alinkutil_config_compiler
在alinkutil_dynamic_compile上面封装了一层，可以将一些配置编译成代码，用作那些极少修改但是读非常频繁的配置。
```
13> alinkutil_config_compiler:compile(a, #{r => j}).
{module,a}
14> a:get().
#{r => j}
15> alinkutil_config_compiler:compile(a, get2, #{r => j}).
{module,a}
16> a:get2().
#{r => j}
```


