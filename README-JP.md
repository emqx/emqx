# EMQ X Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg)](https://coveralls.io/github/emqx/emqx)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack Invite](<https://slack-invite.emqx.io/badge.svg>)](https://slack-invite.emqx.io)
[![Twitter](https://img.shields.io/badge/Twitter-EMQ%20X-1DA1F2?logo=twitter)](https://twitter.com/emqtt)

[![The best IoT MQTT open source team looks forward to your joining](https://www.emqx.io/static/img/github_readme_en_bg.png)](https://www.emqx.io/careers)

[English](./README.md) | [简体中文](./README-CN.md) | 日本語

*EMQ X* ブローカーは、数千万のクライアントを同時に処理できるIoT、M2M、モバイルアプリケーション向けの、完全なオープンソース、高拡張性、高可用性、分散型MQTTメッセージングブローカーです。

バージョン3.0以降、*EMQ X* ブローカーはMQTTV5.0プロトコル仕様を完全にサポートし、MQTT V3.1およびV3.1.1と下位互換性があります。MQTT-SN、CoAP、LwM2M、WebSocket、STOMPなどの通信プロトコルをサポートしています。 1つのクラスター上で1,000万以上の同時MQTT接続に拡張することができます。

- 新機能の一覧については、[EMQ Xリリースノート](https://github.com/emqx/emqx/releases)を参照してください。
- 詳細はこちら[EMQ X公式ウェブサイト](https://www.emqx.io/)をご覧ください。

## インストール

*EMQ X* ブローカーはクロスプラットフォームで、Linux、Unix、macOS、Windowsをサポートしています。これは、*EMQ X* ブローカーをx86_64アーキテクチャサーバー、またはRaspberryPiなどのARMデバイスにデプロイできることを意味します。

#### EMQ X Dockerイメージによるインストール

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### バイナリパッケージによるインストール

対応するオペレーティングシステムのバイナリソフトウェアパッケージは、[EMQ Xのダウンロード](https://www.emqx.io/downloads)ページから取得できます。

- [シングルノードインストール](https://docs.emqx.io/broker/latest/en/getting-started/installation.html)
- [マルチノードインストール](https://docs.emqx.io/broker/latest/en/advanced/cluster.html)

## ソースからビルド

バージョン3.0以降、*EMQ X* ブローカーをビルドするには Erlang/OTP R21+ が必要です。

```
git clone https://github.com/emqx/emqx-rel.git

cd emqx-rel && make

cd _rel/emqx && ./bin/emqx console
```

## クイックスタート

```
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

*EMQ X* ブローカーを起動したら、ブラウザで http://localhost:18083 にアクセスしてダッシュボードを表示できます。

## FAQ

よくある質問については、[EMQ X FAQ](https://docs.emqx.io/broker/latest/en/faq/faq.html)にアクセスしてください。

## ロードマップ

[EMQ X Roadmap uses Github milestones](https://github.com/emqx/emqx/milestones)からプロジェクトの進捗状況を追跡できます。

## コミュニティ、ディスカッション、貢献、サポート

次のチャネルを通じて、EMQコミュニティおよび開発者に連絡できます。

- [Slack](https://slack-invite.emqx.io/)
- [Twitter](https://twitter.com/emqtt)
- [Facebook](https://www.facebook.com/emqxmqtt)
- [Reddit](https://www.reddit.com/r/emqx/)
- [Forum](https://groups.google.com/d/forum/emqtt)
- [Blog](https://medium.com/@emqtt)

バグ、問題、機能のリクエストは、[emqx/emqx](https://github.com/emqx/emqx/issues)に送信してください。

## MQTT仕様

次のリンクから、MQTTプロトコルについて学習および確認できます。

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## License

Apache License 2.0, see [LICENSE](https://github.com/emqx/MQTTX/blob/master/LICENSE).
