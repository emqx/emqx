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

-module(emqx_sn_app).

-behaviour(application).

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

-define(UDP_SOCKOPTS, []).

-type(listener() :: {esockd:proto(), esockd:listen_on(), [esockd:option()]}).

%%--------------------------------------------------------------------
%% Application
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    Addr = application:get_env(emqx_sn, port, 1884),
    GwId = application:get_env(emqx_sn, gateway_id, 1),
    PredefTopics = application:get_env(emqx_sn, predefined, []),
    {ok, Sup} = emqx_sn_sup:start_link(Addr, GwId, PredefTopics),
    start_listeners(),
    {ok, Sup}.

stop(_State) ->
    stop_listeners(),
    ok.

%%--------------------------------------------------------------------
%% Listners
%%--------------------------------------------------------------------

-spec start_listeners() -> ok.
start_listeners() ->
    lists:foreach(fun start_listener/1, listeners_confs()).

-spec start_listener(listener()) -> ok.
start_listener({Proto, ListenOn, Options}) ->
    case start_listener(Proto, ListenOn, Options) of
        {ok, _} -> io:format("Start mqttsn:~s listener on ~s successfully.~n",
                             [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start mqttsn:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason]),
            error(Reason)
    end.

%% Start MQTTSN listener
-spec start_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> {ok, pid()} | {error, term()}.
start_listener(udp, ListenOn, Options) ->
    start_udp_listener('mqttsn:udp', ListenOn, Options);
start_listener(dtls, ListenOn, Options) ->
    start_udp_listener('mqttsn:dtls', ListenOn, Options).

%% @private
start_udp_listener(Name, ListenOn, Options) ->
    SockOpts = esockd:parse_opt(Options),
    esockd:open_udp(Name, ListenOn, merge_default(SockOpts),
                    {emqx_sn_gateway, start_link, [Options -- SockOpts]}).

-spec stop_listeners() -> ok.
stop_listeners() ->
    lists:foreach(fun stop_listener/1, listeners_confs()).

-spec stop_listener(listener()) -> ok | {error, term()}.
stop_listener({Proto, ListenOn, Opts}) ->
    StopRet = stop_listener(Proto, ListenOn, Opts),
    case StopRet of
        ok -> io:format("Stop mqttsn:~s listener on ~s successfully.~n",
                        [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop mqttsn:~s listener on ~s: ~0p~n",
                      [Proto, format(ListenOn), Reason])
    end,
    StopRet.

-spec stop_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> ok | {error, term()}.
stop_listener(udp, ListenOn, _Opts) ->
    esockd:close('mqttsn:udp', ListenOn);
stop_listener(dtls, ListenOn, _Opts) ->
    esockd:close('mqttsn:dtls', ListenOn).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%% @private
%% In order to compatible with the old version of the configuration format
listeners_confs() ->
    ListenOn = application:get_env(emqx_sn, port, 1884),
    GwId = application:get_env(emqx_sn, gateway_id, 1),
    Username = application:get_env(emqx_sn, username, undefined),
    Password = application:get_env(emqx_sn, password, undefined),
    EnableQos3 = application:get_env(emqx_sn, enable_qos3, false),
    EnableStats = application:get_env(emqx_sn, enable_stats, false),
    IdleTimeout = application:get_env(emqx_sn, idle_timeout, 30000),
    SubsResume = application:get_env(emqx_sn, subs_resume, false),
    [{udp, ListenOn, [{gateway_id, GwId},
                      {username, Username},
                      {password, Password},
                      {enable_qos3, EnableQos3},
                      {enable_stats, EnableStats},
                      {idle_timeout, IdleTimeout},
                      {subs_resume, SubsResume},
                      {max_connections, 1024000},
                      {max_conn_rate, 1000},
                      {udp_options, []}]}].

merge_default(Options) ->
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(?UDP_SOCKOPTS, TcpOpts)} | Options1];
        false ->
            [{udp_options, ?UDP_SOCKOPTS} | Options]
    end.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).
