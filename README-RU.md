# Брокер EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://img.shields.io/travis/emqx/emqx?label=Build)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://img.shields.io/coveralls/github/emqx/emqx/master?label=Coverage)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx?label=Docker%20Pulls)](https://hub.docker.com/r/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ%20X-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-EMQ%20X-yellow?logo=github)](https://github.com/emqx/emqx/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

[![The best IoT MQTT open source team looks forward to your joining](https://assets.emqx.com/images/github_readme_en_bg.png)](https://www.emqx.com/en/careers)

[English](./README.md) | [简体中文](./README-CN.md) | [日本語](./README-JP.md) | русский

*EMQX* — это масштабируемый, высоко доступный, распределённый MQTT брокер с полностью открытым кодом для интернета вещей, межмашинного взаимодействия и мобильных приложений, который поддерживает миллионы одновременных подключений.

Начиная с релиза 3.0, брокер *EMQX* полностью поддерживает протокол MQTT версии 5.0, и обратно совместим с версиями 3.1 и 3.1.1, а также протоколами MQTT-SN, CoAP, LwM2M, WebSocket и STOMP. Начиная с релиза 3.0, брокер *EMQX* может масштабироваться до более чем 10 миллионов одновременных MQTT соединений на один кластер.

- Полный список возможностей доступен по ссылке: [EMQX Release Notes](https://github.com/emqx/emqx/releases).
- Более подробная информация доступна на нашем сайте: [EMQX homepage](https://www.emqx.io/).

## Установка

Брокер *EMQX* кросплатформенный, и поддерживает Linux, Unix, macOS и Windows. Он может работать на серверах с архитектурой x86_64 и устройствах на архитектуре ARM, таких как Raspberry Pi.

Более подробная информация о запуске на Windows по ссылке: [Windows.md](./Windows.md)

#### Установка EMQX с помощью Docker-образа

```
docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### Установка бинарного пакета

Сборки для различных операционных систем: [Загрузить EMQX](https://www.emqx.com/en/downloads).

- [Установка на одном сервере](https://www.emqx.io/docs/en/latest/getting-started/install.html)
- [Установка на кластере](https://www.emqx.io/docs/en/latest/advanced/cluster.html)


## Сборка из исходного кода

Начиная с релиза 3.0, для сборки требуется Erlang/OTP R21 или выше.

Инструкция для сборки версии 4.3 и выше:

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin/emqx console
```

Более ранние релизы могут быть собраны с помощью другого репозитория:

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## Первый запуск

Если emqx был собран из исходников: `cd _build/emqx/rel/emqx`.
Или перейдите в директорию, куда emqx был установлен из бинарного пакета.

```bash
# Запуск:
./bin/emqx start

# Проверка статуса:
./bin/emqx_ctl status

# Остановка:
./bin/emqx stop
```

Веб-интерфейс брокера будет доступен по ссылке: http://localhost:18083

## Тесты

### Полное тестирование

```
make eunit ct
```

### Запуск части тестов

Пример:

```bash
make apps/emqx_retainer-ct
```

### Dialyzer
##### Статический анализ всех приложений
```
make dialyzer
```

##### Статический анализ части приложений (список через запятую)
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_authz make dialyzer
```

## Сообщество

### FAQ

Наиболее частые проблемы разобраны в [EMQX FAQ](https://www.emqx.io/docs/en/latest/faq/faq.html).


### Вопросы

Задать вопрос или поделиться идеей можно в [GitHub Discussions](https://github.com/emqx/emqx/discussions).

### Предложения

Более масштабные предложения можно присылать в виде pull request в репозиторий [EIP](https://github.com/emqx/eip).

### Разработка плагинов

Инструкция по разработке собственных плагинов доступна по ссылке: [PLUGIN.md](./PLUGIN.md)

## Спецификации стандарта MQTT

Следующие ссылки содержат спецификации стандартов:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](https://www.oasis-open.org/committees/download.php/66091/MQTT-SN_spec_v1.2.pdf)

## Лицензия

см. [LICENSE](./LICENSE).
