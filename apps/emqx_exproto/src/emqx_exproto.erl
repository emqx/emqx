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

-module(emqx_exproto).

-include("emqx_exproto.hrl").

-export([ start_listeners/0
        , stop_listeners/0
        , start_listener/1
        , start_listener/4
        , stop_listener/4
        , stop_listener/1
        ]).

-export([ start_servers/0
        , stop_servers/0
        , start_server/1
        , stop_server/1
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(start_listeners() -> ok).
start_listeners() ->
    Listeners = application:get_env(?APP, listeners, []),
    NListeners = [start_connection_handler_instance(Listener)
                  || Listener <- Listeners],
    lists:foreach(fun start_listener/1, NListeners).

-spec(stop_listeners() -> ok).
stop_listeners() ->
    Listeners = application:get_env(?APP, listeners, []),
    lists:foreach(fun stop_connection_handler_instance/1, Listeners),
    lists:foreach(fun stop_listener/1, Listeners).

-spec(start_servers() -> ok).
start_servers() ->
    lists:foreach(fun start_server/1, application:get_env(?APP, servers, [])).

-spec(stop_servers() -> ok).
stop_servers() ->
    lists:foreach(fun stop_server/1, application:get_env(?APP, servers, [])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_connection_handler_instance({_Proto, _LisType, _ListenOn, Opts}) ->
    Name = name(_Proto, _LisType),
    {value, {_, HandlerOpts}, LisOpts} = lists:keytake(handler, 1, Opts),
    {SvrAddr, ChannelOptions} = handler_opts(HandlerOpts),
    case emqx_exproto_sup:start_grpc_client_channel(Name, SvrAddr, ChannelOptions) of
        {ok, _ClientChannelPid} ->
            {_Proto, _LisType, _ListenOn, [{handler, Name} | LisOpts]};
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s's connection handler: ~0p~n",
                      [Name, Reason]),
            error(Reason)
    end.

stop_connection_handler_instance({_Proto, _LisType, _ListenOn, _Opts}) ->
    Name = name(_Proto, _LisType),
    _ = emqx_exproto_sup:stop_grpc_client_channel(Name),
    ok.

start_server({Name, Port, SSLOptions}) ->
    case emqx_exproto_sup:start_grpc_server(Name, Port, SSLOptions) of
        {ok, _} ->
            io:format("Start ~s gRPC server on ~w successfully.~n",
                      [Name, Port]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s gRPC server on ~w: ~0p~n",
                      [Name, Port, Reason]),
            error({failed_start_server, Reason})
    end.

stop_server({Name, Port, _SSLOptions}) ->
    ok = emqx_exproto_sup:stop_grpc_server(Name),
    io:format("Stop ~s gRPC server on ~w successfully.~n", [Name, Port]).

start_listener({Proto, LisType, ListenOn, Opts}) ->
    Name = name(Proto, LisType),
    case start_listener(LisType, Name, ListenOn, Opts) of
        {ok, _} ->
            io:format("Start ~s listener on ~s successfully.~n",
                      [Name, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s listener on ~s: ~0p~n",
                      [Name, format(ListenOn), Reason]),
            error(Reason)
    end.

%% @private
start_listener(LisType, Name, ListenOn, LisOpts)
  when LisType =:= tcp;
       LisType =:= ssl ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open(Name, ListenOn, merge_tcp_default(SockOpts),
                {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]});

start_listener(udp, Name, ListenOn, LisOpts) ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open_udp(Name, ListenOn, merge_udp_default(SockOpts),
                    {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]});

start_listener(dtls, Name, ListenOn, LisOpts) ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open_dtls(Name, ListenOn, merge_udp_default(SockOpts),
                    {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]}).

stop_listener({Proto, LisType, ListenOn, Opts}) ->
    Name = name(Proto, LisType),
    StopRet = stop_listener(LisType, Name, ListenOn, Opts),
    case StopRet of
        ok ->
            io:format("Stop ~s listener on ~s successfully.~n",
                      [Name, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop ~s listener on ~s: ~0p~n",
                      [Name, format(ListenOn), Reason])
    end,
    StopRet.

%% @private
stop_listener(_LisType, Name, ListenOn, _Opts) ->
    esockd:close(Name, ListenOn).

%% @private
name(Proto, LisType) ->
    list_to_atom(lists:flatten(io_lib:format("~s:~s", [Proto, LisType]))).

%% @private
format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).

%% @private
merge_tcp_default(Opts) ->
    case lists:keytake(tcp_options, 1, Opts) of
        {value, {tcp_options, TcpOpts}, Opts1} ->
            [{tcp_options, emqx_misc:merge_opts(?TCP_SOCKOPTS, TcpOpts)} | Opts1];
        false ->
            [{tcp_options, ?TCP_SOCKOPTS} | Opts]
    end.

merge_udp_default(Opts) ->
    case lists:keytake(udp_options, 1, Opts) of
        {value, {udp_options, TcpOpts}, Opts1} ->
            [{udp_options, emqx_misc:merge_opts(?UDP_SOCKOPTS, TcpOpts)} | Opts1];
        false ->
            [{udp_options, ?UDP_SOCKOPTS} | Opts]
    end.

%% @private
handler_opts(Opts) ->
    Scheme = proplists:get_value(scheme, Opts),
    Host = proplists:get_value(host, Opts),
    Port = proplists:get_value(port, Opts),
    SvrAddr = lists:flatten(io_lib:format("~s://~s:~w", [Scheme, Host, Port])),
    ClientOpts = case Scheme of
                     https ->
                         SslOpts = lists:keydelete(ssl, 1, proplists:get_value(ssl_options, Opts, [])),
                         #{gun_opts =>
                           #{transport => ssl,
                             transport_opts => SslOpts}};
                     _ -> #{}
                 end,
    {SvrAddr, ClientOpts}.
