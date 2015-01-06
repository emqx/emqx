all: dep compile

compile: dep
	./rebar compile

dep:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf rel/emqtt

dist:
	cd rel && ../rebar generate -f
