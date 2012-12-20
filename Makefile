all: compile

run: compile
	erl -pa ebin -config etc/emqtt.config -s emqtt_app start

compile: deps
	rebar compile

deps:
	rebar get-deps
