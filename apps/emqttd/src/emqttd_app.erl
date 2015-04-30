%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd application.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_app).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(PRINT_MSG(Msg), io:format(Msg)).

-define(PRINT(Format, Args), io:format(Format, Args)).

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

-spec start(StartType, StartArgs) -> {ok, pid()} | {ok, pid(), State} | {error, Reason} when 
    StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term(),
    State     :: term(),
    Reason    :: term().
start(_StartType, _StartArgs) ->
	print_banner(),
    emqttd_mnesia:start(),
    {ok, Sup} = emqttd_sup:start_link(),
	start_servers(Sup),
	{ok, Listeners} = application:get_env(listeners),
    emqttd:load_all_plugins(),
    emqttd:open_listeners(Listeners),
	register(emqttd, self()),
    print_vsn(),
	{ok, Sup}.

print_banner() ->
	?PRINT("starting emqttd on node '~s'~n", [node()]).

print_vsn() ->
	{ok, Vsn} = application:get_key(vsn),
	{ok, Desc} = application:get_key(description),
	?PRINT("~s ~s is running now~n", [Desc, Vsn]).

start_servers(Sup) ->
    Servers = [{"emqttd event", emqttd_event},
               {"emqttd trace", emqttd_trace},
               {"emqttd pooler", {supervisor, emqttd_pooler_sup}},
               {"emqttd client manager", {supervisor, emqttd_cm_sup}},
               {"emqttd session manager", emqttd_sm},
               {"emqttd session supervisor", {supervisor, emqttd_session_sup}},
               {"emqttd pubsub", {supervisor, emqttd_pubsub_sup}},
               {"emqttd stats", emqttd_stats},
               {"emqttd metrics", emqttd_metrics},
               %{"emqttd router", emqttd_router},
               {"emqttd broker", emqttd_broker},
               {"emqttd bridge supervisor", {supervisor, emqttd_bridge_sup}},
               {"emqttd access control", emqttd_access_control},
               {"emqttd system monitor", emqttd_sysmon}],
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

start_child(Sup, {supervisor, Name}) ->
    supervisor:start_child(Sup, supervisor_spec(Name));
start_child(Sup, Name) when is_atom(Name) ->
    {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Name)).

start_child(Sup, {supervisor, Name}, Opts) ->
    supervisor:start_child(Sup, supervisor_spec(Name, Opts));
start_child(Sup, Name, Opts) when is_atom(Name) ->
    {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Name, Opts)).

%%TODO: refactor...
supervisor_spec(Name) ->
    {Name,
        {Name, start_link, []},
            permanent, infinity, supervisor, [Name]}.

supervisor_spec(Name, Opts) ->
    {Name,
        {Name, start_link, [Opts]},
            permanent, infinity, supervisor, [Name]}.

worker_spec(Name) ->
    {Name,
        {Name, start_link, []},
            permanent, 10000, worker, [Name]}.
worker_spec(Name, Opts) -> 
    {Name,
        {Name, start_link, [Opts]},
            permanent, 10000, worker, [Name]}.

-spec stop(State :: term()) -> term().
stop(_State) ->
	{ok, Listeners} = application:get_env(listeners),
    emqttd:close_listeners(Listeners),
    emqttd:unload_all_plugins(),
    ok.

