-module(emqttd_auth_ldap_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqttd_auth_ldap_sup:start_link(),
    Env = application:get_all_env(),
    emqttd_access_control:register_mod(auth, emqttd_auth_ldap, Env),
    {ok, Sup}.

stop(_State) ->
    emqttd_access_control:unregister_mod(auth, emqttd_auth_ldap),
    ok.
