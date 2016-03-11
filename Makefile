.PHONY: rel deps test plugins

APP      = emqttd
BASE_DIR = $(shell pwd)
REBAR    = $(BASE_DIR)/rebar
DIST	 = $(BASE_DIR)/rel/$(APP)

all: submods compile

submods:
	@git submodule update --init

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
	ERL_FLAGS="-config rel/files/emqttd.test.config" $(REBAR) -v skip_deps=true ct
	#$(REBAR) skip_deps=true eunit

edoc:
	@$(REBAR) doc

rel: compile
	@cd rel && $(REBAR) generate -f

plugins:
	@for plugin in ./plugins/* ; do \
	if [ -d $${plugin} ]; then \
		mkdir -p $(DIST)/$${plugin}/ ; \
		cp -R $${plugin}/ebin $(DIST)/$${plugin}/ ; \
		[ -d "$${plugin}/priv" ] && cp -R $${plugin}/priv $(DIST)/$${plugin}/ ; \
		[ -d "$${plugin}/etc" ] && cp -R $${plugin}/etc $(DIST)/$${plugin}/ ; \
		echo "$${plugin} copied" ; \
	fi \
	done

dist: rel plugins

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


