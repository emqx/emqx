all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf rel/emqtt

dist:
	cd rel && ../rebar generate -f
