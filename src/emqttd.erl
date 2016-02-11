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

-module(emqttd).

-export([start/0, env/1, env/2, start_listeners/0, stop_listeners/0,
         load_all_mods/0, is_mod_enabled/1, is_running/1]).

-define(MQTT_SOCKOPTS, [
        binary,
        {packet,    raw},
        {reuseaddr, true},
        {backlog,   512},
        {nodelay,   true}]).

-define(APP, ?MODULE).

-type listener() :: {atom(), inet:port_number(), [esockd:option()]}.

%% @doc Start emqttd application.
-spec start() -> ok | {error, any()}.
start() -> application:start(?APP).

%% @doc Group environment
-spec env(Group :: atom()) -> list().
env(Group) -> application:get_env(?APP, Group, []).

%% @doc Get environment
-spec env(Group :: atom(), Name :: atom()) -> undefined | any().
env(Group, Name) -> proplists:get_value(Name, env(Group)).

%% @doc Start Listeners of the broker.
-spec start_listeners() -> any().
start_listeners() -> lists:foreach(fun start_listener/1, env(listeners)).

%% Start mqtt listener
-spec start_listener(listener()) -> any().
start_listener({mqtt, Port, Opts}) -> start_listener(mqtt, Port, Opts);

%% Start mqtt(SSL) listener
start_listener({mqtts, Port, Opts}) -> start_listener(mqtts, Port, Opts);

%% Start http listener
start_listener({http, Port, Opts}) ->
    mochiweb:start_http(Port, Opts, {emqttd_http, handle_request, []});

%% Start https listener
start_listener({https, Port, Opts}) ->
    mochiweb:start_http(Port, Opts, {emqttd_http, handle_request, []}).

start_listener(Protocol, Port, Opts) ->
    MFArgs = {emqttd_client, start_link, [env(mqtt)]},
    esockd:open(Protocol, Port, merge_sockopts(Opts), MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_opts:merge(?MQTT_SOCKOPTS,
                                 proplists:get_value(sockopts, Options, [])),
    emqttd_opts:merge(Options, [{sockopts, SockOpts}]).

%% @doc Stop Listeners
stop_listeners() -> lists:foreach(fun stop_listener/1, env(listeners)).

stop_listener({Protocol, Port, _Opts}) -> esockd:close({Protocol, Port}).

%% @doc load all modules
load_all_mods() ->
    lists:foreach(fun load_mod/1, env(modules)).

load_mod({Name, Opts}) ->
    Mod = list_to_atom("emqttd_mod_" ++ atom_to_list(Name)),
    case catch Mod:load(Opts) of
        ok               -> lager:info("Load module ~s successfully", [Name]);
        {error, Error}   -> lager:error("Load module ~s error: ~p", [Name, Error]);
        {'EXIT', Reason} -> lager:error("Load module ~s error: ~p", [Name, Reason])
    end.

%% @doc Is module enabled?
-spec is_mod_enabled(Name :: atom()) -> boolean().
is_mod_enabled(Name) -> env(modules, Name) =/= undefined.

%% @doc Is running?
-spec is_running(node()) -> boolean().
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

