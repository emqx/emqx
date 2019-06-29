## shallow clone for speed

REBAR_GIT_CLONE_OPTIONS += --depth 1
export REBAR_GIT_CLONE_OPTIONS

SUITES_FILES := $(shell find test -name '*_SUITE.erl')

CT_SUITES := $(foreach value,$(SUITES_FILES),$(shell val=$$(basename $(value) .erl); echo $${val%_*}))

CT_NODE_NAME = emqxct@127.0.0.1

RUN_NODE_NAME = emqxdebug@127.0.0.1

.PHONY: all
all: compile

.PHONY: run
run: run_setup unlock
	@rebar3 as test get-deps
	@rebar3 as test auto --name $(RUN_NODE_NAME) --script test/run_emqx.escript

.PHONY: run_setup
run_setup:
	@erl -noshell -eval \
	    "{ok, [[HOME]]} = init:get_argument(home), \
		 FilePath = HOME ++ \"/.config/rebar3/rebar.config\", \
		 case file:consult(FilePath) of \
             {ok, Term} -> \
				 NewTerm = case lists:keyfind(plugins, 1, Term) of \
	                           false -> [{plugins, [rebar3_auto]} | Term]; \
	                	  	   {plugins, OldPlugins} -> \
		          		           NewPlugins0 = OldPlugins -- [rebar3_auto], \
	             	     	       NewPlugins = [rebar3_auto | NewPlugins0], \
                                   lists:keyreplace(plugins, 1, Term, {plugins, NewPlugins}) \
	                       end, \
	             ok = file:write_file(FilePath, [io_lib:format(\"~p.\n\", [I]) || I <- NewTerm]); \
            _Enoent -> \
		        os:cmd(\"mkdir -p ~/.config/rebar3/ \"), \
	            NewTerm=[{plugins, [rebar3_auto]}], \
	            ok = file:write_file(FilePath, [io_lib:format(\"~p.\n\", [I]) || I <- NewTerm]) \
	     end, \
	     halt(0)."

.PHONY: shell
shell:
	@rebar3 as test auto

compile: unlock
	@rebar3 compile

unlock:
	@rebar3 unlock

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
	@rm -rf Mnesia.*
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish erl_crash.dump
