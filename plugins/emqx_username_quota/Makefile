REBAR ?= rebar3

.PHONY: rel
rel:
	$(REBAR) emqx_plugrel tar

.PHONY: ct
ct:
	$(REBAR) as test ct -v
