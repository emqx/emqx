## shallow clone for speed

REBAR_GIT_CLONE_OPTIONS += --depth 1
export REBAR_GIT_CLONE_OPTIONS

REBAR = rebar3
all: compile

compile:
	$(REBAR) compile
	cp -r _build/default/lib/emqx_mini_plugin/ebin ./

clean: distclean

ct: compile
	$(REBAR) as test ct -v

eunit: compile
	$(REBAR) as test eunit

xref:
	$(REBAR) xref

distclean:
	@rm -rf _build
	@rm -f ebin/ data/app.*.config data/vm.*.args rebar.lock
