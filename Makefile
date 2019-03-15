.PHONY: deps test

BASE_DIR = $(shell pwd)
REBAR=$(BASE_DIR)/rebar

all: deps compile xref

deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref skip_deps=true

clean:
	@$(REBAR) clean

test:
	@$(REBAR) skip_deps=true ct

edoc:
	@$(REBAR) doc

PLT  = $(BASE_DIR)/.ecpool_dialyzer.plt
APPS = erts kernel stdlib sasl crypto syntax_tools ssl public_key mnesia inets compiler

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) deps/*/ebin ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) deps/*/ebin ebin

dialyzer: compile
	dialyzer -Wno_return --plt $(PLT) deps/*/ebin ebin

