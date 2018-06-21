.PHONY: plugins tests

PROJECT = emqx
PROJECT_DESCRIPTION = EMQ X Broker
PROJECT_VERSION = 3.0

NO_AUTOPATCH = gen_rpc cuttlefish

DEPS = goldrush gproc lager esockd ekka mochiweb pbkdf2 lager_syslog bcrypt clique jsx canal_lock

dep_goldrush     = git https://github.com/basho/goldrush 0.1.9
dep_gproc        = git https://github.com/uwiger/gproc 0.7.0
dep_jsx          = git https://github.com/talentdeficit/jsx 2.9.0
dep_getopt       = git https://github.com/jcomellas/getopt v0.8.2
dep_lager        = git https://github.com/basho/lager master
dep_lager_syslog = git https://github.com/basho/lager_syslog
dep_esockd       = git https://github.com/emqtt/esockd emqx30
dep_ekka         = git https://github.com/emqtt/ekka develop
dep_mochiweb     = git https://github.com/emqtt/mochiweb emqx30
dep_pbkdf2       = git https://github.com/emqtt/pbkdf2 2.0.1
dep_bcrypt       = git https://github.com/smarkets/erlang-bcrypt master
dep_clique       = git https://github.com/emqtt/clique
dep_clique       = git https://github.com/emqtt/clique
dep_canal_lock   = git https://github.com/emqx/canal-lock

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

TEST_DEPS = emqx_ct_helplers
dep_emqx_ct_helplers = git git@github.com:emqx/emqx_ct_helpers

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

CT_SUITES = emqx_inflight
## emqx_trie emqx_router emqx_frame emqx_mqtt_compat

#CT_SUITES = emqx emqx_broker emqx_mod emqx_lib emqx_topic emqx_mqueue emqx_inflight \
#			emqx_vm emqx_net emqx_protocol emqx_access emqx_router

CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqxct@127.0.0.1

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl lager compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx.conf -i priv/emqx.schema -d data/

