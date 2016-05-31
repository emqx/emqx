%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-include("emqttd_cli.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-export([start_listener/1, stop_listener/1, is_mod_enabled/1]).

%% MQTT SockOpts
-define(MQTT_SOCKOPTS, [
        binary,
        {packet,    raw},
        {reuseaddr, true},
        {backlog,   512},
        {nodelay,   true}]).

-type listener() :: {atom(), esockd:listen_on(), [esockd:option()]}.

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

-spec(start(StartType, StartArgs) -> {ok, pid()} | {ok, pid(), State} | {error, Reason} when
      StartType :: normal | {takeover, node()} | {failover, node()},
      StartArgs :: term(),
      State     :: term(),
      Reason    :: term()).
start(_StartType, _StartArgs) ->
    print_banner(),
    emqttd_mnesia:start(),
    {ok, Sup} = emqttd_sup:start_link(),
    start_servers(Sup),
    emqttd_cli:load(),
    load_all_mods(),
    emqttd_plugins:load(),
    start_listeners(),
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
    ?PRINT("starting emqttd on node '~s'~n", [node()]).

print_vsn() ->
    {ok, Vsn} = application:get_key(vsn),
    {ok, Desc} = application:get_key(description),
    ?PRINT("~s ~s is running now~n", [Desc, Vsn]).

%%--------------------------------------------------------------------
%% Start Servers
%%--------------------------------------------------------------------

start_servers(Sup) ->
    Servers = [{"emqttd ctl", emqttd_ctl},
               {"emqttd hook", emqttd_hook},
               {"emqttd pubsub", {supervisor, emqttd_pubsub_sup}},
               {"emqttd stats", emqttd_stats},
               {"emqttd metrics", emqttd_metrics},
               {"emqttd retainer", emqttd_retainer},
               {"emqttd pooler", {supervisor, emqttd_pooler}},
               {"emqttd trace", {supervisor, emqttd_trace_sup}},
               {"emqttd client manager", {supervisor, emqttd_cm_sup}},
               {"emqttd session manager", {supervisor, emqttd_sm_sup}},
               {"emqttd session supervisor", {supervisor, emqttd_session_sup}},
               {"emqttd broker", emqttd_broker},
               {"emqttd alarm", emqttd_alarm},
               {"emqttd mod supervisor", emqttd_mod_sup},
               {"emqttd bridge supervisor", {supervisor, emqttd_bridge_sup}},
               {"emqttd access control", emqttd_access_control},
               {"emqttd system monitor", {supervisor, emqttd_sysmon_sup}}],
    [start_server(Sup, Server) || Server <- Servers].

start_server(_Sup, {Name, F}) when is_function(F) ->
    ?PRINT("~s is starting...", [Name]),
    F(),
    ?PRINT_MSG("[done]~n");

start_server(Sup, {Name, Server}) ->
    ?PRINT("~s is starting...", [Name]),
    start_child(Sup, Server),
    ?PRINT_MSG("[done]~n");

start_server(Sup, {Name, Server, Opts}) ->
    ?PRINT("~s is starting...", [ Name]),
    start_child(Sup, Server, Opts),
    ?PRINT_MSG("[done]~n").

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
%% Load Modules
%%--------------------------------------------------------------------

%% @doc load all modules
load_all_mods() ->
    lists:foreach(fun load_mod/1, emqttd:env(modules)).

load_mod({Name, Opts}) ->
    Mod = list_to_atom("emqttd_mod_" ++ atom_to_list(Name)),
    case catch Mod:load(Opts) of
        ok               -> lager:info("Load module ~s successfully", [Name]);
        {error, Error}   -> lager:error("Load module ~s error: ~p", [Name, Error]);
        {'EXIT', Reason} -> lager:error("Load module ~s error: ~p", [Name, Reason])
    end.

%% @doc Is module enabled?
-spec(is_mod_enabled(Name :: atom()) -> boolean()).
is_mod_enabled(Name) -> emqttd:env(modules, Name) =/= undefined.

%%--------------------------------------------------------------------
%% Start Listeners
%%--------------------------------------------------------------------

%% @doc Start Listeners of the broker.
-spec(start_listeners() -> any()).
start_listeners() -> lists:foreach(fun start_listener/1, emqttd:env(listeners)).

%% Start mqtt listener
-spec(start_listener(listener()) -> any()).
start_listener({mqtt, ListenOn, Opts}) -> start_listener(mqtt, ListenOn, Opts);

%% Start mqtt(SSL) listener
start_listener({mqtts, ListenOn, Opts}) -> start_listener(mqtts, ListenOn, Opts);

%% Start http listener
start_listener({http, ListenOn, Opts}) ->
    mochiweb:start_http(http, ListenOn, Opts, {emqttd_http, handle_request, []});

%% Start https listener
start_listener({https, ListenOn, Opts}) ->
    mochiweb:start_http(https, ListenOn, Opts, {emqttd_http, handle_request, []}).

start_listener(Protocol, ListenOn, Opts) ->
    MFArgs = {emqttd_client, start_link, [emqttd:env(mqtt)]},
    esockd:open(Protocol, ListenOn, merge_sockopts(Opts), MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_opts:merge(?MQTT_SOCKOPTS,
                                 proplists:get_value(sockopts, Options, [])),
    emqttd_opts:merge(Options, [{sockopts, SockOpts}]).

%%--------------------------------------------------------------------
%% Stop Listeners
%%--------------------------------------------------------------------

%% @doc Stop Listeners
stop_listeners() -> lists:foreach(fun stop_listener/1, emqttd:env(listeners)).

%% @private
stop_listener({Protocol, ListenOn, _Opts}) -> esockd:close(Protocol, ListenOn).

