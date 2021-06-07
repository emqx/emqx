REBAR := rebar3

.PHONY: all
all: es

.PHONY: compile
compile:
	$(REBAR) compile

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build erl_crash.dump rebar3.crashdump

.PHONY: xref
xref:
	$(REBAR) xref

.PHONY: eunit
eunit: compile
	$(REBAR) eunit -v -c
	$(REBAR) cover

.PHONY: ct
ct: compile
	$(REBAR) as test ct -v

cover:
	$(REBAR) cover

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer

.PHONY: es
es: compile
	$(REBAR) escriptize

.PHONY: elvis
elvis:
	./scripts/elvis-check.sh
