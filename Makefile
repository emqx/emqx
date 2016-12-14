PROJECT = emqttd
PROJECT_DESCRIPTION = Erlang MQTT Broker
PROJECT_VERSION = 2.0.4

DEPS = gproc lager gen_logger esockd mochiweb

dep_gproc       = git https://github.com/uwiger/gproc
dep_getopt      = git https://github.com/jcomellas/getopt v0.8.2
dep_lager       = git https://github.com/basho/lager master
dep_gen_logger  = git https://github.com/emqtt/gen_logger
dep_esockd      = git https://github.com/emqtt/esockd master
dep_mochiweb    = git https://github.com/emqtt/mochiweb

ERLC_OPTS += +'{parse_transform, lager_transform}'

TEST_DEPS = cuttlefish emqttc
dep_cuttlefish = git https://github.com/emqtt/cuttlefish
dep_emqttc = git https://github.com/emqtt/emqttc

NO_AUTOPATCH = cuttlefish

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose
# EUNIT_ERL_OPTS =

CT_SUITES = emqttd emqttd_access emqttd_lib emqttd_mod emqttd_net \
            emqttd_mqueue emqttd_protocol emqttd_topic emqttd_trie \
            emqttd_vm

CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqttd_ct@127.0.0.1

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	cuttlefish -l info -e etc/ -c etc/emq.conf -i priv/emq.schema -d data/

