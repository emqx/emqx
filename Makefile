all: get-deps compile

compile: get-deps
	./rebar compile

get-deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf rel/emqtt

dist:
	cd rel && ../rebar generate -f
