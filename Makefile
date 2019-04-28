## shallow clone for speed

REBAR_GIT_CLONE_OPTIONS += --depth 1
export REBAR_GIT_CLONE_OPTIONS

# CT_SUITES = emqx_trie emqx_router emqx_frame emqx_mqtt_compat

CT_SUITES = emqx emqx_client emqx_zone emqx_banned emqx_session \
			emqx_broker emqx_cm emqx_frame emqx_guid emqx_inflight emqx_json \
			emqx_keepalive emqx_lib emqx_metrics emqx_mod emqx_mod_sup emqx_mqtt_caps \
			emqx_mqtt_props emqx_mqueue emqx_net emqx_pqueue emqx_router emqx_sm \
			emqx_tables emqx_time emqx_topic emqx_trie emqx_vm emqx_mountpoint \
			emqx_listeners emqx_protocol emqx_pool emqx_shared_sub emqx_bridge \
			emqx_hooks emqx_batch emqx_sequence emqx_pmon emqx_pd emqx_gc emqx_ws_connection \
			emqx_packet emqx_connection emqx_tracer emqx_sys_mon emqx_message emqx_os_mon \
            emqx_vm_mon emqx_alarm_handler emqx_rpc emqx_flapping

CT_NODE_NAME = emqxct@127.0.0.1

compile:
	@rebar3 compile

clean: gen-clean

.PHONY: gen-clean
gen-clean:
	@rm -f etc/emqx.conf.rendered

## Cuttlefish escript is built by default when cuttlefish app (as dependency) was built
CUTTLEFISH_SCRIPT := _build/default/lib/cuttlefish/cuttlefish

.PHONY: cover
cover:
	@rebar3 cover

.PHONY: coveralls
coveralls:
	@rebar3 coveralls send

.PHONY: xref
xref:
	@rebar3 xref

.PHONY: deps
deps:
	@rebar3 get-deps

.PHONY: eunit
eunit:
	@rebar3 eunit -v

.PHONY: ct-setup
ct-setup:
	@mkdir -p data
	@[ ! -f data/loaded_plugins ] && touch data/loaded_plugins
	@ln -s -f '../../../../etc' _build/test/lib/emqx/
	@ln -s -f '../../../../data' _build/test/lib/emqx/

.PHONY: ct
ct: ct-setup
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(shell echo $(foreach var,$(CT_SUITES),test/$(var)_SUITE) | tr ' ' ',')

## Run one single CT with rebar3
## e.g. make ct-one-suite suite=emqx_bridge
.PHONY: ct-one-suite
ct-one-suite: ct-setup
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(suite)_SUITE

.PHONY: clean
clean:
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish
