Русский | [简体中文](./README-CN.md) | [English](./README.md)

# Брокер EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml/badge.svg)](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![X](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=x)](https://x.com/EMQTech)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)


*EMQX* — это самый масштабируемый и популярный высокопроизводительный MQTT брокер с полностью открытым кодом для интернета вещей, межмашинного взаимодействия и мобильных приложений. EMQX может поддерживать более чем 100 миллионов одновременных соединенией на одном кластере с задержкой в 1 миллисекунду, а также принимать и обрабабывать миллионы MQTT сообщений в секунду.

Мы [протестировали масштабируемость](https://www.emqx.com/en/blog/reaching-100m-mqtt-connections-with-emqx-5-0) EMQX v5.0 и подтвердили что брокер может поддерживать до 100 миллионов одновременных подключений устройств. Это является критически важной вехой для разработчиков IoT. EMQX 5.0 также поставляется с множеством интересных новых функций и значительными улучшениями производительности, включая более мощный [механизм правил](https://www.emqx.com/en/solutions/iot-rule-engine), улучшенное управление безопасностью, расширение базы данных Mria и многое другое для повышения масштабируемости приложений IoT.

За последние несколько лет EMQX приобрел популярность среди IoT-компаний и используется более чем 20 000 пользователей по всему миру из более чем 60 стран, при этом по всему миру поддерживается более 250 миллионов подключений к IoT-устройствам.

Для получения дополнительной информации, пожалуйста, посетите [домашнюю страницу EMQX](https://www.emqx.com/).

## Начало работы

#### EMQX Cloud

Самый простой способ запустить EMQX это развернуть его с помощью EMQX Cloud. Вы можете [попробовать EMQX Cloud бесплатно](https://www.emqx.com/en/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud-intl.emqx.com/console/deployments/0?oper=new), данные кредитной карточки не требуются.

#### Установка EMQX с помощью Docker

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx:latest
```

Далее, следуйте, пожалуйста [руководству по установке EMQX с помощью Docker](https://docs.emqx.com/en/emqx/latest/deploy/install-docker.html).

#### Запуск кластера EMQX на Kubernetes

Пожалуйста, ознакомьтесь с официальной [документацией для EMQX Operator](https://docs.emqx.com/en/emqx-operator/latest/getting-started/getting-started.html).

#### Дополнительные опции установки

Если вы предпочитаете устанавливать и управлять EMQX самостоятельно, вы можете загрузить последнюю версию с [официального сайта](https://www.emqx.com/en/downloads-and-install/enterprise).

Смотрите также [EMQX installation documentation](https://docs.emqx.com/en/emqx/latest/deploy/install.html).

## Документация

Документация по EMQX: [docs.emqx.com/en/emqx/latest](https://docs.emqx.com/en/emqx/latest/).

Документация по EMQX Cloud: [docs.emqx.com/en/cloud/latest](https://docs.emqx.com/en/cloud/latest/).

## Участие в разработке

Пожалуйста, прочитайте [contributing.md](./CONTRIBUTING.md).

Для более организованных предложений по улучшению вы можете отправить pull requests в [EIP](https://github.com/emqx/eip).

## Присоединяйтесь к коммьюнити

- Подпишитесь на [@EMQTech on Twitter](https://twitter.com/EMQTech).
- Подключайтесь к [обсуждениям](https://github.com/emqx/emqx/discussions) на Github, если у вас есть какой-то вопрос.
- Присоединяйтесь к нашему [официальному Discord](https://discord.gg/xYGf3fQnES), чтобы поговорить с командой разработки.
- Подписывайтесь на канал [EMQX YouTube](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q).

## Дополнительные ресурсы

- [MQTT client programming](https://www.emqx.com/en/blog/category/mqtt-programming)

  Коллекция блогов, чтобы помочь разработчикам быстро начать работу с MQTT на PHP, Node.js, Python, Golang, и других языках программирования.

- [MQTT SDKs](https://www.emqx.com/en/mqtt-client-sdk)

  Мы выбрали популярные SDK клиентов MQTT на различных языках программирования и предоставили примеры кода, которые помогут вам быстро понять, как использовать клиенты MQTT.

- [MQTTX](https://mqttx.app/)

  Элегантный кроссплатформенный клиент MQTT 5.0, в виде десктопного приложения, приложения для командной строки и веб-приложения, чтобы помочь вам быстрее разрабатывать и отлаживать службы и приложения MQTT.

- [Internet of Vehicles](https://www.emqx.com/en/blog/category/internet-of-vehicles)

  Создайте надежную, эффективную и специализированную для вашей индустрии платформу IoV на основе практического опыта EMQ, от теоретических знаний, таких как выбор протокола, до практических операций, таких как проектирование архитектуры платформы.

## Сборка из исходного кода

Ветка `master` предназначена для последней версии 5, переключитесь на ветку `main-v4.4` для версии 4.4.

* EMQX требует OTP 24 для версии 4.4.
* Версии 5.0 ~ 5.3 могут быть собраны с OTP 24 или 25.
* Версия 5.4 и новее могут быть собраны с OTP 25 или 26.

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin/emqx console
```

Версии до 4.2 (включительно) нужно собирать из другого репозитория:

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## Лицензия

см. [LICENSE](./LICENSE).
