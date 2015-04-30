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
	@cd rel && ../rebar generate -f

plugins:
	@for plugin in ./plugins/* ; do \
		cp -R $${plugin} $(DIST)/plugins/ && rm -rf $(DIST)/$${plugin}/src/ ; \
	done

dist: rel plugins

