%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqttd_app).

-behaviour(application).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd_cli.hrl").

-include("emqttd_protocol.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-export([start_listener/1, stop_listener/1, restart_listener/1]).

-type(listener() :: {atom(), esockd:listen_on(), [esockd:option()]}).

-define(APP, emqttd).

%%--------------------------------------------------------------------
%% Application Callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    print_banner(),
    ekka:start(),
    {ok, Sup} = emqttd_sup:start_link(),
    start_servers(Sup),
    emqttd_cli:load(),
    register_acl_mod(),
    start_autocluster(),
    register(emqttd, self()),
    print_vsn(),
    {ok, Sup}.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    catch stop_listeners().

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
    ?PRINT("starting ~s on node '~s'~n", [?APP, node()]).

print_vsn() ->
    {ok, Vsn} = application:get_key(vsn),
    ?PRINT("~s ~s is running now~n", [?APP, Vsn]).

%%--------------------------------------------------------------------
%% Start Servers
%%--------------------------------------------------------------------

start_servers(Sup) ->
    Servers = [{"emqttd ctl", emqttd_ctl},
               {"emqttd hook", emqttd_hooks},
               {"emqttd router", emqttd_router},
               {"emqttd pubsub", {supervisor, emqttd_pubsub_sup}},
               {"emqttd stats", emqttd_stats},
               {"emqttd metrics", emqttd_metrics},
               {"emqttd pooler", {supervisor, emqttd_pooler}},
               {"emqttd trace", {supervisor, emqttd_trace_sup}},
               {"emqttd client manager", {supervisor, emqttd_cm_sup}},
               {"emqttd session manager", {supervisor, emqttd_sm_sup}},
               {"emqttd session supervisor", {supervisor, emqttd_session_sup}},
               {"emqttd wsclient supervisor", {supervisor, emqttd_ws_client_sup}},
               {"emqttd broker", emqttd_broker},
               {"emqttd alarm", emqttd_alarm},
               {"emqttd mod supervisor", emqttd_mod_sup},
               {"emqttd bridge supervisor", {supervisor, emqttd_bridge_sup_sup}},
               {"emqttd access control", emqttd_access_control},
               {"emqttd system monitor", {supervisor, emqttd_sysmon_sup}}],
    [start_server(Sup, Server) || Server <- Servers].

start_server(_Sup, {Name, F}) when is_function(F) ->
    ?PRINT("~s is starting...", [Name]),
    F(),
    ?PRINT_MSG("[ok]~n");

start_server(Sup, {Name, Server}) ->
    ?PRINT("~s is starting...", [Name]),
    start_child(Sup, Server),
    ?PRINT_MSG("[ok]~n");

start_server(Sup, {Name, Server, Opts}) ->
    ?PRINT("~s is starting...", [ Name]),
    start_child(Sup, Server, Opts),
    ?PRINT_MSG("[ok]~n").

start_child(Sup, {supervisor, Module}) ->
    supervisor:start_child(Sup, supervisor_spec(Module));

start_child(Sup, Module) when is_atom(Module) ->
    {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Module)).

start_child(Sup, {supervisor, Module}, Opts) ->
    supervisor:start_child(Sup, supervisor_spec(Module, Opts));

start_child(Sup, Module, Opts) when is_atom(Module) ->
    supervisor:start_child(Sup, worker_spec(Module, Opts)).

supervisor_spec(Module) when is_atom(Module) ->
    supervisor_spec(Module, start_link, []).

supervisor_spec(Module, Opts) ->
    supervisor_spec(Module, start_link, [Opts]).

supervisor_spec(M, F, A) ->
    {M, {M, F, A}, permanent, infinity, supervisor, [M]}.

worker_spec(Module) when is_atom(Module) ->
    worker_spec(Module, start_link, []).

worker_spec(Module, Opts) when is_atom(Module) ->
    worker_spec(Module, start_link, [Opts]).

worker_spec(M, F, A) ->
    {M, {M, F, A}, permanent, 10000, worker, [M]}.

%%--------------------------------------------------------------------
%% Register default ACL File
%%--------------------------------------------------------------------

register_acl_mod() ->
    case emqttd:env(acl_file) of
        {ok, File} -> emqttd_access_control:register_mod(acl, emqttd_acl_internal, [File]);
        undefined  -> ok
    end.

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------

start_autocluster() ->
    ekka:callback(prepare, fun emqttd:shutdown/1),
    ekka:callback(reboot,  fun emqttd:reboot/0),
    ekka:autocluster(?APP, fun after_autocluster/0).

after_autocluster() ->
    emqttd_plugins:init(),
    emqttd_plugins:load(),
    start_listeners().

%%--------------------------------------------------------------------
%% Start Listeners
%%--------------------------------------------------------------------

%% @doc Start Listeners of the broker.
-spec(start_listeners() -> any()).
start_listeners() -> lists:foreach(fun start_listener/1, emqttd:env(listeners, [])).

%% Start mqtt listener
-spec(start_listener(listener()) -> any()).
start_listener({tcp, ListenOn, Opts}) ->
    start_listener('mqtt:tcp', ListenOn, Opts);

%% Start mqtt(SSL) listener
start_listener({ssl, ListenOn, Opts}) ->
    start_listener('mqtt:ssl', ListenOn, Opts);

%% Start http listener
start_listener({Proto, ListenOn, Opts}) when Proto == http; Proto == ws ->
    mochiweb:start_http('mqtt:ws', ListenOn, Opts, {emqttd_ws, handle_request, []});

%% Start https listener
start_listener({Proto, ListenOn, Opts}) when Proto == https; Proto == wss ->
    mochiweb:start_http('mqtt:wss', ListenOn, Opts, {emqttd_ws, handle_request, []});

start_listener({Proto, ListenOn, Opts}) when Proto == api ->
    mochiweb:start_http('mqtt:api', ListenOn, Opts, emqttd_http:http_handler()).

start_listener(Proto, ListenOn, Opts) ->
    Env = lists:append(emqttd:env(client, []), emqttd:env(protocol, [])),
    MFArgs = {emqttd_client, start_link, [Env]},
    {ok, _} = esockd:open(Proto, ListenOn, merge_sockopts(Opts), MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_misc:merge_opts(
                 ?MQTT_SOCKOPTS, proplists:get_value(sockopts, Options, [])),
    emqttd_misc:merge_opts(Options, [{sockopts, SockOpts}]).

%%--------------------------------------------------------------------
%% Stop Listeners
%%--------------------------------------------------------------------

%% @doc Stop Listeners
stop_listeners() -> lists:foreach(fun stop_listener/1, emqttd:env(listeners, [])).


%% @private
stop_listener({tcp, ListenOn, _Opts}) ->
    esockd:close('mqtt:tcp', ListenOn);
stop_listener({ssl, ListenOn, _Opts}) ->
    esockd:close('mqtt:ssl', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:stop_http('mqtt:ws', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:stop_http('mqtt:wss', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == api ->
    mochiweb:stop_http('mqtt:api', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) ->
    esockd:close(Proto, ListenOn).

%% @doc Restart Listeners
restart_listener({tcp, ListenOn, _Opts}) ->
    esockd:reopen('mqtt:tcp', ListenOn);
restart_listener({ssl, ListenOn, _Opts}) ->
    esockd:reopen('mqtt:ssl', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:restart_http('mqtt:ws', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:restart_http('mqtt:wss', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == api ->
    mochiweb:restart_http('mqtt:api', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) ->
    esockd:reopen(Proto, ListenOn).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
merge_sockopts_test_() ->
    Opts =  [{acceptors, 16}, {max_clients, 512}],
    ?_assert(merge_sockopts(Opts) == [{sockopts, ?MQTT_SOCKOPTS} | Opts]).

-endif.
