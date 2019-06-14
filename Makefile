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
			emqx_hooks emqx_batch emqx_sequence emqx_pmon emqx_pd emqx_gc emqx_ws_channel \
			emqx_packet emqx_channel emqx_tracer emqx_sys_mon emqx_message emqx_os_mon \
            emqx_vm_mon emqx_alarm_handler emqx_rpc emqx_flapping

CT_NODE_NAME = emqxct@127.0.0.1

compile:
	@rebar3 compile

clean: distclean

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

.PHONY: ct_setup
ct_setup:
	rebar3 as test compile
	@mkdir -p data
	@if [ ! -f data/loaded_plugins ]; then touch data/loaded_plugins; fi
	@ln -s -f '../../../../etc' _build/test/lib/emqx/
	@ln -s -f '../../../../data' _build/test/lib/emqx/

.PHONY: ct
ct: ct_setup
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(shell echo $(foreach var,$(CT_SUITES),test/$(var)_SUITE) | tr ' ' ',')

## Run one single CT with rebar3
## e.g. make ct-one-suite suite=emqx_bridge
.PHONY: $(SUITES:%=ct-%)
$(CT_SUITES:%=ct-%): ct_setup
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(@:ct-%=%)_SUITE

.PHONY: app.config
app.config: $(CUTTLEFISH_SCRIPT) etc/gen.emqx.conf
	$(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/gen.emqx.conf -i priv/emqx.schema -d data/

$(CUTTLEFISH_SCRIPT):
	@rebar3 get-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

bbmustache:
	@git clone https://github.com/soranoba/bbmustache.git && cd bbmustache && ./rebar3 compile && cd ..

# This hack is to generate a conf file for testing
# relx overlay is used for release
etc/gen.emqx.conf: bbmustache etc/emqx.conf
	@erl -noshell -pa bbmustache/_build/default/lib/bbmustache/ebin -eval \
		"{ok, Temp} = file:read_file('etc/emqx.conf'), \
		{ok, Vars0} = file:consult('vars'), \
		Vars = [{atom_to_list(N), list_to_binary(V)} || {N, V} <- Vars0], \
		Targ = bbmustache:render(Temp, Vars), \
		ok = file:write_file('etc/gen.emqx.conf', Targ), \
		halt(0)."

.PHONY: gen-clean
gen-clean:
	@rm -rf bbmustache
	@rm -f etc/gen.emqx.conf etc/emqx.conf.rendered

.PHONY: distclean
distclean: gen-clean
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish erl_crash.dump
