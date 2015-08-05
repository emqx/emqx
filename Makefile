.PHONY: rel deps test plugins

APP		 = emqttd
BASE_DIR = $(shell pwd)
REBAR    = $(BASE_DIR)/rebar
DIST	 = $(BASE_DIR)/rel/$(APP)

all: deps compile

compile: deps
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps

xref:
	@$(REBAR) xref skip_deps=true

clean:
	@$(REBAR) clean

test:
	@$(REBAR) skip_deps=true eunit

edoc:
	@$(REBAR) doc

rel: compile
	@cd rel && $(REBAR) generate -f

plugins:
	@for plugin in ./plugins/* ; do \
	if [ -d $${plugin} ]; then \
		echo "copy $${plugin}"; \
		cp -R $${plugin} $(DIST)/plugins/ && rm -rf $(DIST)/$${plugin}/src/ ; \
	fi \
	done

dist: rel plugins

PLT  = $(BASE_DIR)/.emqttd_dialyzer.plt
APPS = erts kernel stdlib sasl crypto ssl os_mon syntax_tools \
	   public_key mnesia inets compiler

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) \
		deps/*/ebin ./ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) \
		deps/*/ebin ./ebin

dialyzer: compile
	dialyzer -Wno_return --plt $(PLT) deps/*/ebin ./ebin

