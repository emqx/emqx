# EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://img.shields.io/travis/emqx/emqx?label=Build)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://img.shields.io/coveralls/github/emqx/emqx/master?label=Coverage)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx?label=Docker%20Pulls)](https://hub.docker.com/r/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ%20X-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![Twitter](https://img.shields.io/badge/Twitter-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

[![The best IoT MQTT open source team looks forward to your joining](https://assets.emqx.com/images/github_readme_en_bg.png)](https://www.emqx.com/en/careers)

[English](./README.md) | [简体中文](./README-CN.md) | 日本語 | [русский](./README-RU.md)

*EMQX* は、高い拡張性と可用性をもつ、分散型のMQTTブローカーです。数千万のクライアントを同時に処理するIoT、M2M、モバイルアプリケーション向けです。

version 3.0 以降、*EMQX* は MQTT V5.0 の仕様を完全にサポートしており、MQTT V3.1およびV3.1.1とも下位互換性があります。
MQTT-SN、CoAP、LwM2M、WebSocket、STOMPなどの通信プロトコルをサポートしています。 MQTTの同時接続数は1つのクラスター上で1,000万以上にまでスケールできます。

- 新機能の一覧については、[EMQXリリースノート](https://github.com/emqx/emqx/releases)を参照してください。
- 詳細はこちら[EMQX公式ウェブサイト](https://www.emqx.io/)をご覧ください。

## インストール

*EMQX* はクロスプラットフォームで、Linux、Unix、macOS、Windowsをサポートしています。
そのため、x86_64アーキテクチャサーバー、またはRaspberryPiなどのARMデバイスに *EMQX* をデプロイすることもできます。

Windows上における *EMQX* のビルドと実行については、[Windows.md](./Windows.md)をご参照ください。

#### Docker イメージによる EMQX のインストール

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### バイナリパッケージによるインストール

それぞれのOSに対応したバイナリソフトウェアパッケージは、[EMQXのダウンロード](https://www.emqx.com/en/downloads)ページから取得できます。

- [シングルノードインストール](https://www.emqx.io/docs/en/latest/getting-started/install.html)
- [マルチノードインストール](https://www.emqx.io/docs/en/latest/advanced/cluster.html)

## ソースからビルド

version 3.0 以降の *EMQX* をビルドするには Erlang/OTP R21+ が必要です。

version 4.3 以降の場合：

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## クイックスタート

emqx をソースコードからビルドした場合は、
`cd _build/emqx/rel/emqx`でリリースビルドのディレクトリに移動してください。

リリースパッケージからインストールした場合は、インストール先のルートディレクトリに移動してください。

```
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

*EMQX* の起動後、ブラウザで http://localhost:18083 にアクセスするとダッシュボードが表示されます。

## テスト

### 全てのテストケースを実行する

```
make eunit ct
```

### common test の一部を実行する

```bash
make apps/emqx_retainer-ct
```

### Dialyzer
##### アプリケーションの型情報を解析する
```
make dialyzer
```

##### 特定のアプリケーションのみ解析する（アプリケーション名をコンマ区切りで入力）
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_authz make dialyzer
```

## コミュニティ

### FAQ

よくある質問については、[EMQX FAQ](https://www.emqx.io/docs/en/latest/faq/faq.html)をご確認ください。

### 質問する

質問や知識共有の場として[GitHub Discussions](https://github.com/emqx/emqx/discussions)を用意しています。

### 提案

大規模な改善のご提案がある場合は、[EIP](https://github.com/emqx/eip)にPRをどうぞ。

### 自作プラグイン

プラグインを自作することができます。[PLUGIN.md](./PLUGIN.md) をご確認ください。


## MQTTの仕様について

下記のサイトで、MQTTのプロトコルについて学習・確認できます。

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](https://www.oasis-open.org/committees/download.php/66091/MQTT-SN_spec_v1.2.pdf)

## License

See [LICENSE](./LICENSE).
