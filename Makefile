.PHONY: test

REBAR=./rebar

all: get-deps compile

compile: get-deps
	@$(REBAR) compile

get-deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps

xref:
	@$(REBAR) xref skip_deps=true

clean:
	@$(REBAR) clean
	rm -rf rel/emqttd

test:
	@$(REBAR) skip_deps=true eunit

edoc:
	@$(REBAR) doc

dist:
	#TODO write new Makefile
	cd rel && ../rebar generate -f
	cp -R plugins/emqttd_plugin_demo rel/emqttd/plugins/ && rm -rf rel/emqttd/plugins/emqttd_plugin_demo/src

