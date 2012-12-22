-module(emqtt_auth_anonymous).

-export([init/1,
		add/2,
		check/2,
		delete/1]).

init(_Opts) -> ok.

check(_, _) -> true.

add(_, _) -> ok.

delete(_Username) -> ok.
