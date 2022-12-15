EMQX的配置文件格式是 [HOCON](https://github.com/emqx/hocon) 。
HOCON（Human-Optimized Config Object Notation）是一个JSON的超集，非常适用于易于人类读写的配置数据存储。

## 分层结构

EMQX的配置文件可分为三层，自底向上依次是：

1. 不可变的基础层 `emqx.conf` 加上 `EMQX_` 前缀的环境变量。<br/>
   修改这一层的配置之后，需要重启节点来使之生效。
1. 集群范围重载层：`$EMQX_NODE__DATA_DIR/configs/cluster-override.conf`
1. 节点本地重载层：`$EMQX_NODE__DATA_DIR/configs/local-override.conf`

如果环境变量 `$EMQX_NODE__DATA_DIR` 没有设置，那么该目录会从 `emqx.conf` 的 `node.data_dir` 配置中读取。

配置文件 `cluster-override.conf` 的内容会在运行时被EMQX重写。
这些重写发生在 dashboard UI，管理HTTP API，或者CLI对集群配置进行修改时。
当EMQX运行在集群中时，一个EMQX节点重启之后，会从集群中其他节点复制该文件内容到本地。

:::tip Tip
有些配置项是不能被重载的（例如 `node.name`）。
配置项如果有 `mapping: path.to.boot.config.key` 这个属性，
则不能被添加到重载文件 `*-override.conf` 中。
:::

更多的重载规则，请参考下文 [配置重载规则](#配置重载规则)。

## 配置文件语法

在配置文件中，值可以被记为类似JSON的对象，例如

```
node {
    name = "emqx@127.0.0.1"
    cookie = "mysecret"
}
```

另一种等价的表示方法是扁平的，例如

```
node.name = "127.0.0.1"
node.cookie = "mysecret"
```

这种扁平格式几乎与EMQX的配置文件格式向后兼容
在4.x系列中（所谓的'cuttlefish'格式）。

它并不是完全兼容，因为HOCON经常要求字符串两端加上引号。
而cuttlefish把`=`符右边的所有字符都视为值。

例如，cuttlefish：`node.name = emqx@127.0.0.1`，HOCON：`node.name = "emqx@127.0.0.1"`。

没有特殊字符的字符串在HOCON中也可以不加引号。
例如：`foo`，`foo_bar`和`foo_bar_1`。

关于更多的HOCON语法，请参考[规范](https://github.com/lightbend/config/blob/main/HOCON.md)

## Schema

为了使HOCON对象类型安全，EMQX为它引入了一个schema。
该schema定义了数据类型，以及数据字段的名称和元数据，用于配置值的类型检查等等。

::: tip Tip
当前阅读到配置文件的文档本身就是由模式元数据生成的。
:::

### 复杂数据类型

EMQX的配置文件中，有4中复杂数据结构类型，它们分别是：

1. Struct：结构体都是有类型名称的，结构体中可以有任意多个字段。
   结构体和字段的名称由不带特殊字符的全小些字母组成，名称中可以带数字，但不得以数字开头，多个单词可用下划线分隔。
1. Map: Map 与 Struct（结构体）类似，但是内部的字段不是预先定义好的。
1. Union: 联合 `MemberType1 | MemberType2 | ...`，可以理解为：“不是这个，就是那个”
1. Array: 数组 `[ElementType]`

::: tip Tip
如果Map的字段名称是纯数字，它会被解释成一个数组。
例如
```
myarray.1 = 74
myarray.2 = 75
```
会被解析成 `myarray = [74, 75]`。这个用法在重载数组元素的值时候非常有用。
:::

### 原始数据类型

复杂类型定义了数据 "盒子"，其中可能包含其他复杂数据或原始值。
有很多不同的原始类型，仅举几个例子。

* 原子 `atom()`。
* 布尔 `boolean()`。
* 字符串 `string()`。
* 整形 `integer()`。
* 浮点数 `float()`。
* 数值 `number()`。
* 二进制编码的字符串 `binary()` 是 `string()` 的另一种格式。
* 时间间隔 `emqx_schema:duration()` 是 `integer()` 的另一种格式。
* ...

::: tip Tip
原始类型的名称大多是自我描述的，所以不需要过多的注释。
但是有一些不是那么直观的数据类型，则需要配合字段的描述文档进行理解。
:::


### 配置路径

如果我们把EMQX的配置值理解成一个类似目录树的结构，那么类似于文件系统中使用斜杠或反斜杠进行层级分割，
EMQX使用的配置路径的层级分割符是 `'.'`

被 `'.'` 号分割的每一段，则是 Struct（结构体）的字段，或 Map 的 key。

下面有几个例子：

```
node.name = "emqx.127.0.0.1"
zone.zone1.max_packet_size = "10M"
authentication.1.enable = true
```

### 环境变量重载

因为 `'.'`  分隔符不能使用于环境变量，所以我们需要使用另一个分割符。EMQX选用的是双下划线 `__`。
为了与其他的环境变量有所区分，EMQX还增加了一个前缀 `EMQX_` 来用作环境变量命名空间。

例如 `node.name` 的重载变量名是 `EMQX_NODE__NAME`。

环境变量的值，是按 HOCON 值解析的，这也使得环境变量可以用来传递复杂数据类型的值。

例如，下面这个环境变量传入一个数组类型的值。

```
export EMQX_LISTENERS__SSL__L1__AUTHENTICATION__SSL__CIPHERS='["TLS_AES_256_GCM_SHA384"]'
```

这也意味着有些带特殊字符（例如`:` 和 `=`），则需要用双引号对这个值包起来。

例如`localhost:1883` 会被解析成一个结构体 `{"localhost": 1883}`。
想要把它当字符串使用时，就必需使用引号，如下：

```
EMQX_BRIDGES__MQTT__MYBRIDGE__CONNECTOR_SERVER='"localhost:1883"'
```


::: tip Tip
未定义的根路径会被EMQX忽略，例如 `EMQX_UNKNOWN_ROOT__FOOBAR` 这个环境变量会被EMQX忽略，
因为 `UNKNOWN_ROOT` 不是预先定义好的根路径。
对于已知的根路径，未知的字段名称将被记录为warning日志，比如下面这个例子。

```
[warning] unknown_env_vars: ["EMQX_AUTHENTICATION__ENABLED"]
```

这是因为正确的字段名称是 `enable`，而不是 `enabled`。
:::

### 配置重载规则

HOCON的值是分层覆盖的，普遍规则如下：

- 在同一个文件中，后（在文件底部）定义的值，覆盖前（在文件顶部）到值。
- 当按层级覆盖时，高层级的值覆盖低层级的值。

结下来的文档将解释更详细的规则。

#### 结构体

合并覆盖规则。在如下配置中，最后一行的 `debug` 值会覆盖覆盖原先`level`字段的 `error` 值，但是 `enable` 字段保持不变。
```
log {
    console_handler{
        enable=true,
        level=error
    }
}

## 控制台日志打印先定义为 `error` 级，后被覆写成 `debug` 级

log.console_handler.level=debug
```

#### Map

Map与结构体类似，也是合并覆盖规则。
如下例子中，`zone1` 的 `max_packet_size` 可以在文件后面覆写。

```
zone {
    zone1 {
        mqtt.max_packet_size = 1M
    }
}

## 报文大小限制最先被设置成1MB，后被覆写为10MB

zone.zone1.mqtt.max_packet_size = 10M
```

#### 数组元素

如上面介绍过，EMQX配置中的数组有两种表达方式。

* 列表格式，例如： `[1, 2, 3]`
* 带下标的Map格式，例如： `{"1"=1, "2"=2, "3"=3}`

点好（`'.'`）分隔到路径中的纯数字会被解析成数组下标。
例如，`authentication.1={...}`  会被解析成 `authentication={"1": {...}}`，进而进一步解析成 `authentication=[{...}]`
有了这个特性，我们就可以轻松覆写数组某个元素的值，例如：

```
authentication=[{enable=true, backend="built_in_database", mechanism="password_based"}]
# 可以用下面的方式将第一个元素的 `enable` 字段覆写
authentication.1.enable=false
```

::: warning Warning
使用列表格式是的数组将全量覆写原值，如下例：

```
authentication=[{enable=true, backend="built_in_database", mechanism="password_based"}]
## 下面这中方式会导致数组第一个元素的除了 `enable` 以外的其他字段全部丢失
authentication=[{enable=true}]
```
:::

#### TLS/SSL ciphers

从 v5.0.6 开始 EMQX 不在配置文件中详细列出所有默认的密码套件名称。
而是在配置文件中使用一个空列表，然后在运行时替换成默认的密码套件。

下面这些密码套件是 EMQX 默认支持的：

tlsv1.3:
```
ciphers =
  [ "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256", "TLS_AES_128_CCM_SHA256",
    "TLS_AES_128_CCM_8_SHA256"
  ]
```

tlsv1.2 或更早

```
ciphers =
  [ "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-ECDSA-AES256-SHA384",
    "ECDHE-RSA-AES256-SHA384",
    "ECDH-ECDSA-AES256-GCM-SHA384",
    "ECDH-RSA-AES256-GCM-SHA384",
    "ECDH-ECDSA-AES256-SHA384",
    "ECDH-RSA-AES256-SHA384",
    "DHE-DSS-AES256-GCM-SHA384",
    "DHE-DSS-AES256-SHA256",
    "AES256-GCM-SHA384",
    "AES256-SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES128-SHA256",
    "ECDHE-RSA-AES128-SHA256",
    "ECDH-ECDSA-AES128-GCM-SHA256",
    "ECDH-RSA-AES128-GCM-SHA256",
    "ECDH-ECDSA-AES128-SHA256",
    "ECDH-RSA-AES128-SHA256",
    "DHE-DSS-AES128-GCM-SHA256",
    "DHE-DSS-AES128-SHA256",
    "AES128-GCM-SHA256",
    "AES128-SHA256",
    "ECDHE-ECDSA-AES256-SHA",
    "ECDHE-RSA-AES256-SHA",
    "DHE-DSS-AES256-SHA",
    "ECDH-ECDSA-AES256-SHA",
    "ECDH-RSA-AES256-SHA",
    "ECDHE-ECDSA-AES128-SHA",
    "ECDHE-RSA-AES128-SHA",
    "DHE-DSS-AES128-SHA",
    "ECDH-ECDSA-AES128-SHA",
    "ECDH-RSA-AES128-SHA"
  ]
```

配置 PSK 认证的监听器

```
ciphers = [
  [ "RSA-PSK-AES256-GCM-SHA384",
    "RSA-PSK-AES256-CBC-SHA384",
    "RSA-PSK-AES128-GCM-SHA256",
    "RSA-PSK-AES128-CBC-SHA256",
    "RSA-PSK-AES256-CBC-SHA",
    "RSA-PSK-AES128-CBC-SHA",
    "PSK-AES256-GCM-SHA384",
    "PSK-AES128-GCM-SHA256",
    "PSK-AES256-CBC-SHA384",
    "PSK-AES256-CBC-SHA",
    "PSK-AES128-CBC-SHA256",
    "PSK-AES128-CBC-SHA"
  ]
```
