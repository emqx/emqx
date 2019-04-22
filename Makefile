
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

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling -Wrace_conditions #-Wunmatched_returns

compile:
	@rebar3 compile

clean: gen-clean

.PHONY: gen-clean
gen-clean:
	@rm -f etc/gen.emqx.conf

## bbmustache is a mustache template library used to render templated config files
## for common tests.
BBMUSTACHE := _build/test/lib/bbmustache
$(BBMUSTACHE):
	@rebar3 as test compile

## Cuttlefish escript is built by default when cuttlefish app (as dependency) was built
CUTTLEFISH_SCRIPT := _build/default/lib/cuttlefish/cuttlefish

app.config: etc/gen.emqx.conf
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/gen.emqx.conf -i priv/emqx.schema -d data/

## NOTE: Mustache templating was resolved by relx overlay when building a release.
## This is only to generate a conf file for testing,
etc/gen.emqx.conf: $(BBMUSTACHE) etc/emqx.conf
	@$(verbose) erl -noshell -pa _build/test/lib/bbmustache/ebin -eval \
		"{ok, Temp} = file:read_file('etc/emqx.conf'), \
		{ok, Vars0} = file:consult('vars'), \
		Vars = [{atom_to_list(N), list_to_binary(V)} || {N, V} <- Vars0], \
		Targ = bbmustache:render(Temp, Vars), \
		ok = file:write_file('etc/gen.emqx.conf', Targ), \
		halt(0)."

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

## 'ct-setup' is a pre hook for 'rebar3 ct',
## but not the makefile target ct's dependency
## because 'ct-setup' requires test dependencies to be compiled first
.PHONY: ct-setup
ct-setup:
	@rebar3 as test compile
	@ln -s -f '../../../../etc' _build/test/lib/emqx/
	@ln -s -f '../../../../data' _build/test/lib/emqx/

.PHONY: ct
ct: app.config ct-setup
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
