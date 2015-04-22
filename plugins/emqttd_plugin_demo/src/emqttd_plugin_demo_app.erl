-module(emqttd_plugin_demo_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqttd_plugin_demo_sup:start_link(),
    emqttd_access_control:register_mod(auth, emqttd_plugin_demo_auth, []),
    emqttd_access_control:register_mod(acl, emqttd_plugin_demo_acl, []),
    {ok, Sup}.

stop(_State) ->
    emqttd_access_control:unregister_mod(auth, emqttd_plugin_demo_auth),
    emqttd_access_control:unregister_mod(acl, emqttd_plugin_demo_acl),
    ok.
