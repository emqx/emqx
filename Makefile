all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

generate:
	./rebar generate -f

relclean:
	rm -rf rel/emqtt
