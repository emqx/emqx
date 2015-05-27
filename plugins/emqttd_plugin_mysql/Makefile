REBAR?=./rebar


all: build


clean:
	$(REBAR) clean
	rm -rf logs
	rm -rf .eunit
	rm -f test/*.beam


distclean: clean
	git clean -fxd

build: depends
	$(REBAR) compile


eunit:
	$(REBAR) eunit skip_deps=true


check: build eunit


%.beam: %.erl
	erlc -o test/ $<


.PHONY: all clean distclean depends build eunit check
