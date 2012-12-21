-module(emqtt).

-export([start/0]).

start() ->
	ok = application:start(sasl),
	mnesia:create_schema([node()]),	
	mnesia:start(),
	lager:start(),
	ok = application:start(emqtt).

