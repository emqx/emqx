-module(emqttd_auth_mysql_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqttd_auth_mysql_sup:start_link(),
    emqttd_access_control:register_mod(auth, emqttd_auth_mysql, []),
    {ok, Sup}.

stop(_State) ->
    emqttd_access_control:unregister_mod(auth, emqttd_auth_mysql),
    ok.
