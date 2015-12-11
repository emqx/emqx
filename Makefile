.PHONY: rel deps test

APP      = emqttd
BASE_DIR = $(shell pwd)
REBAR 	?= $(shell which rebar3 || echo ./rebar3)
DIST	 = $(BASE_DIR)/rel/$(APP)

all: submods compile

submods:
	@git submodule update --init

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref skip_deps=true

clean:
	@$(REBAR) clean

test:
	@$(REBAR) skip_deps=true eunit

edoc:
	@$(REBAR) edoc

rel:
	@$(REBAR) as prod release tar

dist: rel

PLT  = $(BASE_DIR)/.emqttd_dialyzer.plt
APPS = erts kernel stdlib sasl crypto ssl os_mon syntax_tools \
	   public_key mnesia inets compiler

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) \
		deps/*/ebin ./ebin plugins/*/ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) \
		deps/*/ebin ./ebin plugins/*/ebin

dialyzer: compile
	dialyzer -Wno_return --plt $(PLT) deps/*/ebin ./ebin plugins/*/ebin


