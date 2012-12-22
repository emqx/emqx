-module(emqtt_auth_anonymous).

-export([init/1,
		add/2,
		check/2,
		delete/1]).

init(_Opts) ->
	ok.

check(Username, _) when is_binary(Username) ->
	true.

add(Username, _Password) when is_binary(Username) ->
	ok.

delete(Username) when is_binary(Username) ->
	ok.
