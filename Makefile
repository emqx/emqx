PROJECT = emqx
PROJECT_DESCRIPTION = EMQ X Broker
PROJECT_VERSION = 2.3.2

NO_AUTOPATCH = cuttlefish

DEPS = goldrush gproc lager esockd ekka mochiweb pbkdf2 lager_syslog bcrypt clique jsx

dep_goldrush     = git https://github.com/basho/goldrush 0.1.9
dep_gproc        = git https://github.com/uwiger/gproc
dep_getopt       = git https://github.com/jcomellas/getopt v0.8.2
dep_lager        = git https://github.com/basho/lager master
dep_lager_syslog = git https://github.com/basho/lager_syslog
dep_jsx          = git https://github.com/talentdeficit/jsx
dep_esockd       = git https://github.com/emqtt/esockd v5.1
dep_ekka         = git https://github.com/emqtt/ekka master
dep_mochiweb     = git https://github.com/emqtt/mochiweb v4.2.0
dep_pbkdf2       = git https://github.com/emqtt/pbkdf2 2.0.1
dep_bcrypt       = git https://github.com/smarkets/erlang-bcrypt master
dep_clique       = git https://github.com/emqtt/clique

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

TEST_DEPS = emqttc emq_dashboard
dep_emqttc = git https://github.com/emqtt/emqttc
dep_emq_dashboard = git https://github.com/emqtt/emq_dashboard develop

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

CT_SUITES = emqx emqx_mod emqx_lib emqx_topic emqx_trie emqx_mqueue emqx_inflight \
			emqx_vm emqx_net emqx_protocol emqx_access emqx_config emqx_router

CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqxct@127.0.0.1

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl lager compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx.conf -i priv/emqx.schema -d data/

