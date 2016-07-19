PROJECT = emqttd
PROJECT_DESCRIPTION = Erlang MQTT Broker
PROJECT_VERSION = 2.0

DEPS = gproc lager gen_logger gen_conf esockd mochiweb

dep_gproc      = git https://github.com/uwiger/gproc.git
dep_lager      = git https://github.com/basho/lager.git
dep_gen_conf   = git https://github.com/emqtt/gen_conf.git
dep_gen_logger = git https://github.com/emqtt/gen_logger.git
dep_esockd     = git https://github.com/emqtt/esockd.git udp
dep_mochiweb   = git https://github.com/emqtt/mochiweb.git

ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose
EUNIT_ERL_OPTS = -args_file rel/vm.args -config rel/sys.config

CT_SUITES = emqttd emqttd_access emqttd_backend emqttd_lib emqttd_mod emqttd_net \
			emqttd_mqueue emqttd_protocol emqttd_topic emqttd_trie
CT_OPTS = -cover test/ct.cover.spec -erl_args -name emqttd_ct@127.0.0.1 -config rel/sys.config

COVER = true

include erlang.mk

app:: rebar.config

