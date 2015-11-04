
-module(emqttd_auth_dashboard).

%% Auth callbacks
-export([init/1, check/3, description/0]).

init(Opts) ->
    {ok, Opts}.

check(_Client, _Password, _Opts) ->
    allow.

description() ->
    "Test emqttd_auth_dashboard Mod".
