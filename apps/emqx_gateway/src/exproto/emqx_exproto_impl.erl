%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The ExProto Gateway Implement interface
-module(emqx_exproto_impl).

-behaviour(emqx_gateway_impl).

%% APIs
-export([ reg/0
        , unreg/0
        ]).

-export([ on_gateway_load/2
        , on_gateway_update/3
        , on_gateway_unload/2
        ]).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

reg() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:reg(exproto, RegistryOptions).

unreg() ->
    emqx_gateway_registry:unreg(exproto).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

start_grpc_server(_GwName, undefined) ->
    undefined;
start_grpc_server(GwName, Options = #{bind := ListenOn}) ->
    Services = #{protos => [emqx_exproto_pb],
                 services => #{
                   'emqx.exproto.v1.ConnectionAdapter' => emqx_exproto_gsvr}
                },
    SvrOptions = case maps:to_list(maps:get(ssl, Options, #{})) of
                  [] -> [];
                  SslOpts ->
                      [{ssl_options, SslOpts}]
              end,
    case grpc:start_server(GwName, ListenOn, Services, SvrOptions) of
        {ok, _SvrPid} ->
            console_print("Start ~ts gRPC server on ~p successfully.",
                          [GwName, ListenOn]);
        {error, Reason} ->
            ?ELOG("Falied to start ~ts gRPC server on ~p, reason: ~p",
                  [GwName, ListenOn, Reason])
    end.

stop_grpc_server(GwName) ->
    _ = grpc:stop_server(GwName),
    console_print("Stop ~s gRPC server successfully.~n", [GwName]).

start_grpc_client_channel(_GwName, undefined) ->
    undefined;
start_grpc_client_channel(GwName, Options = #{address := Address}) ->
    {Host, Port} = emqx_gateway_utils:parse_address(Address),
    case maps:to_list(maps:get(ssl, Options, #{})) of
        [] ->
            SvrAddr = compose_http_uri(http, Host, Port),
            grpc_client_sup:create_channel_pool(GwName, SvrAddr, #{});
        SslOpts ->
            ClientOpts = #{gun_opts =>
                           #{transport => ssl,
                             transport_opts => SslOpts}},
            SvrAddr = compose_http_uri(https, Host, Port),
            grpc_client_sup:create_channel_pool(GwName, SvrAddr, ClientOpts)
    end.

compose_http_uri(Scheme, Host, Port) ->
    lists:flatten(
      io_lib:format(
        "~s://~s:~w", [Scheme, Host, Port])).

stop_grpc_client_channel(GwName) ->
    _ = grpc_client_sup:stop_channel_pool(GwName),
    ok.

on_gateway_load(_Gateway = #{ name := GwName,
                              config := Config
                            }, Ctx) ->
    %% XXX: How to monitor it ?
    %% Start grpc client pool & client channel
    PoolName = pool_name(GwName),
    PoolSize = emqx_vm:schedulers() * 2,
    {ok, PoolSup} = emqx_pool_sup:start_link(
                      PoolName, hash, PoolSize,
                      {emqx_exproto_gcli, start_link, []}),
    _ = start_grpc_client_channel(GwName,
                                  maps:get(handler, Config, undefined)
                                 ),
    %% XXX: How to monitor it ?
    _ = start_grpc_server(GwName, maps:get(server, Config, undefined)),

    NConfig = maps:without(
                 [server, handler],
                 Config#{pool_name => PoolName}
                ),
    Listeners = emqx_gateway_utils:normalize_config(
                  NConfig#{handler => GwName}
                 ),
    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(GwName, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _GwState = #{ctx => Ctx, pool => PoolSup}}.

on_gateway_update(Config, Gateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(Gateway, GwState),
        on_gateway_load(Gateway#{config => Config}, Ctx)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update ~ts; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [GwName, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_gateway_unload(_Gateway = #{ name := GwName,
                                config := Config
                              }, _GwState = #{pool := PoolSup}) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    %% Stop funcs???
    exit(PoolSup, kill),
    stop_grpc_server(GwName),
    stop_grpc_client_channel(GwName),
    lists:foreach(fun(Lis) ->
        stop_listener(GwName, Lis)
    end, Listeners).

pool_name(GwName) ->
    list_to_atom(lists:concat([GwName, "_gcli_pool"])).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(GwName, Ctx, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            console_print("Gateway ~ts:~ts:~ts on ~ts started.~n",
                          [GwName, Type, LisName, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start gateway ~ts:~ts:~ts on ~ts: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) ->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    NCfg = Cfg#{
             ctx => Ctx,
             listener => {GwName, Type, LisName},
             frame_mod => emqx_exproto_frame,
             chann_mod => emqx_exproto_channel
            },
    MFA = {emqx_gateway_conn, start_link, [NCfg]},
    NSockOpts = merge_default_by_type(Type, SocketOpts),
    do_start_listener(Type, Name, ListenOn, NSockOpts, MFA).

do_start_listener(Type, Name, ListenOn, Opts, MFA)
  when Type == tcp;
       Type == ssl ->
    esockd:open(Name, ListenOn, Opts, MFA);
do_start_listener(udp, Name, ListenOn, Opts, MFA) ->
    esockd:open_udp(Name, ListenOn, Opts, MFA);
do_start_listener(dtls, Name, ListenOn, Opts, MFA) ->
    esockd:open_dtls(Name, ListenOn, Opts, MFA).

merge_default_by_type(Type, Options) when Type =:= tcp;
                                          Type =:= ssl ->
    Default = emqx_gateway_utils:default_tcp_options(),
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{tcp_options, Default} | Options]
    end;
merge_default_by_type(Type, Options) when Type =:= udp;
                                          Type =:= dtls ->
    Default = emqx_gateway_utils:default_udp_options(),
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{udp_options, Default} | Options]
    end.

stop_listener(GwName, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(GwName, Type, LisName, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok ->
            console_print("Gateway ~ts:~ts:~ts on ~ts stopped.~n",
                          [GwName, Type, LisName, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop gateway ~ts:~ts:~ts on ~ts: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(GwName, Type, LisName, ListenOn, _SocketOpts, _Cfg) ->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    esockd:close(Name, ListenOn).

-ifndef(TEST).
console_print(Fmt, Args) -> ?ULOG(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.
