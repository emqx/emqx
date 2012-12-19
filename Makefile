all: compile

run: compile
	erl -pa ebin -config etc/emqtt.config -s emqtt_app start

compile:
	rebar compile
