%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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

-author('feng@emqtt.io').

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
	{ok, Listeners} = application:get_env(listen),
    emqttd:open(Listeners),
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
    {ok, SessOpts} = application:get_env(session),
    {ok, PubSubOpts} = application:get_env(pubsub),
    {ok, BrokerOpts} = application:get_env(broker),
    {ok, MetricOpts} = application:get_env(metrics),
    {ok, AccessOpts} = application:get_env(access_control),
    Servers = [
            {"emqttd config", emqttd_config},
            {"emqttd event", emqttd_event},
            {"emqttd pooler", {supervisor, emqttd_pooler_sup}},
            {"emqttd client manager", {supervisor, emqttd_cm_sup}},
            {"emqttd session manager", emqttd_sm},
            {"emqttd session supervisor", {supervisor, emqttd_session_sup}, SessOpts},
            {"emqttd pubsub", {supervisor, emqttd_pubsub_sup}, PubSubOpts},
            %{"emqttd router", emqttd_router},
            {"emqttd broker", emqttd_broker, BrokerOpts},
            {"emqttd metrics", emqttd_metrics, MetricOpts},
            {"emqttd bridge supervisor", {supervisor, emqttd_bridge_sup}},
            {"emqttd access control", emqttd_access_control, AccessOpts},
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
            permanent, 5000, worker, [Name]}.
worker_spec(Name, Opts) -> 
    {Name,
        {Name, start_link, [Opts]},
            permanent, 5000, worker, [Name]}.

-spec stop(State :: term()) -> term().
stop(_State) ->
    ok.

