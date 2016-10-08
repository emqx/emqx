PROJECT = emqttd
PROJECT_DESCRIPTION = Erlang MQTT Broker
PROJECT_VERSION = 3.0

DEPS = gproc lager gen_logger esockd mochiweb getopt pbkdf2 \
	   clique time_compat rand_compat

dep_gproc       = git https://github.com/uwiger/gproc
dep_getopt      = git https://github.com/jcomellas/getopt v0.8.2
dep_lager       = git https://github.com/basho/lager master
dep_gen_logger  = git https://github.com/emqtt/gen_logger
dep_esockd      = git https://github.com/emqtt/esockd emq20
dep_mochiweb    = git https://github.com/emqtt/mochiweb
dep_clique      = git https://github.com/basho/clique
dep_pbkdf2	    = git https://github.com/basho/erlang-pbkdf2 2.0.0
dep_time_compat = git https://github.com/lasp-lang/time_compat
dep_rand_compat = git https://github.com/lasp-lang/rand_compat

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/basho/cuttlefish master

ERLC_OPTS += +'{parse_transform, lager_transform}'

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose
# EUNIT_ERL_OPTS =

CT_SUITES = emqttd emqttd_access emqttd_lib emqttd_mod emqttd_net \
			emqttd_mqueue emqttd_protocol emqttd_topic emqttd_trie
CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqttd_ct@127.0.0.1

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	cuttlefish -l info -e etc/ -c etc/emqttd.conf -i priv/emqttd.schema -d data

