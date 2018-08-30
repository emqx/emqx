.PHONY: plugins tests

PROJECT = emqx
PROJECT_DESCRIPTION = EMQ X Broker
PROJECT_VERSION = 3.0

DEPS = jsx gproc gen_rpc lager ekka esockd cowboy clique lager_syslog

dep_jsx     = git https://github.com/talentdeficit/jsx 2.9.0
dep_gproc   = git https://github.com/uwiger/gproc 0.8.0
dep_gen_rpc = git https://github.com/emqx/gen_rpc 2.2.0
dep_lager   = git https://github.com/erlang-lager/lager 3.6.4
dep_esockd  = git https://github.com/emqx/esockd emqx30
dep_ekka    = git https://github.com/emqx/ekka emqx30
dep_cowboy  = git https://github.com/ninenines/cowboy 2.4.0
dep_clique  = git https://github.com/emqx/clique
dep_lager_syslog = git https://github.com/basho/lager_syslog 3.0.1

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqx/cuttlefish emqx30

#TEST_DEPS = emqx_ct_helplers
#dep_emqx_ct_helplers = git git@github.com:emqx/emqx-ct-helpers

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

# CT_SUITES = emqx_mqueue
## emqx_trie emqx_router emqx_frame emqx_mqtt_compat

CT_SUITES = emqx emqx_connection emqx_session emqx_access emqx_base62 emqx_broker emqx_client emqx_cm emqx_frame emqx_guid emqx_inflight \
			emqx_json emqx_keepalive emqx_lib emqx_metrics emqx_misc emqx_mod emqx_mqtt_caps \
			emqx_mqtt_compat emqx_mqtt_properties emqx_mqueue emqx_message emqx_net emqx_pqueue emqx_router emqx_sm \
			emqx_stats emqx_tables emqx_time emqx_topic emqx_trie emqx_vm emqx_zone emqx_mountpoint

CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqxct@127.0.0.1

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl lager compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling -Wrace_conditions #-Wunmatched_returns

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx.conf -i priv/emqx.schema -d data/

