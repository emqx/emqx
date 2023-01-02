%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp).

-behaviour(application).
-behaviour(supervisor).

-emqx_plugin(protocol).

-export([ start/2
        , stop/1
        ]).

-export([ start_listeners/0
        , start_listener/1
        , start_listener/3
        , stop_listeners/0
        , stop_listener/1
        , stop_listener/3
        ]).

-export([force_clear_after_app_stoped/0]).

-export([init/1]).

-define(APP, ?MODULE).
-define(TCP_OPTS, [binary, {packet, raw}, {reuseaddr, true}, {nodelay, true}]).

-type(listener() :: {esockd:proto(), esockd:listen_on(), [esockd:option()]}).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = supervisor:start_link({local, emqx_stomp_sup}, ?MODULE, []),
    start_listeners(),
    {ok, Sup}.

stop(_State) ->
    stop_listeners().

force_clear_after_app_stoped() ->
    lists:foreach(fun({Name = {ProtoName, _}, _}) ->
        case is_stomp_listener(ProtoName) of
            true -> esockd:close(Name);
            _ -> ok
        end
    end, esockd:listeners()).

is_stomp_listener('stomp:tcp') -> true;
is_stomp_listener('stomp:ssl') -> true;
is_stomp_listener(_) -> false.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 10, 100}, []}}.

%%--------------------------------------------------------------------
%% Start/Stop listeners
%%--------------------------------------------------------------------

-spec(start_listeners() -> ok).
start_listeners() ->
    lists:foreach(fun start_listener/1, listeners_confs()).

-spec(start_listener(listener()) -> ok).
start_listener({Proto, ListenOn, Options}) ->
    case start_listener(Proto, ListenOn, Options) of
        {ok, _} -> io:format("Start stomp:~s listener on ~s successfully.~n",
                             [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start stomp:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason]),
            error(Reason)
    end.

-spec(start_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> {ok, pid()} | {error, term()}).
start_listener(tcp, ListenOn, Options) ->
    start_stomp_listener('stomp:tcp', ListenOn, Options);
start_listener(ssl, ListenOn, Options) ->
    start_stomp_listener('stomp:ssl', ListenOn, Options).

%% @private
start_stomp_listener(Name, ListenOn, Options) ->
    SockOpts = esockd:parse_opt(Options),
    esockd:open(Name, ListenOn, merge_default(SockOpts),
                 {emqx_stomp_connection, start_link, [Options -- SockOpts]}).

-spec(stop_listeners() -> ok).
stop_listeners() ->
    lists:foreach(fun stop_listener/1, listeners_confs()).

-spec(stop_listener(listener()) -> ok | {error, term()}).
stop_listener({Proto, ListenOn, Opts}) ->
    StopRet = stop_listener(Proto, ListenOn, Opts),
    case StopRet of
        ok -> io:format("Stop stomp:~s listener on ~s successfully.~n",
                        [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop stomp:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason])
    end,
    StopRet.

-spec(stop_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> ok | {error, term()}).
stop_listener(tcp, ListenOn, _Opts) ->
    esockd:close('stomp:tcp', ListenOn);
stop_listener(ssl, ListenOn, _Opts) ->
    esockd:close('stomp:ssl', ListenOn).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

listeners_confs() ->
    {ok, {Port, Opts}} = application:get_env(?APP, listener),
    Options = application:get_env(?APP, frame, []),
    Anonymous = application:get_env(emqx_stomp, allow_anonymous, false),
    {ok, DefaultUser} = application:get_env(emqx_stomp, default_user),
    [{tcp, Port, [{allow_anonymous, Anonymous},
                  {default_user, DefaultUser} | Options ++ Opts]}].

merge_default(Options) ->
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(?TCP_OPTS, TcpOpts)} | Options1];
        false ->
            [{tcp_options, ?TCP_OPTS} | Options]
    end.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).
