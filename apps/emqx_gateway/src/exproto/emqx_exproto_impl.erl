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

-behavior(emqx_gateway_impl).

%% APIs
-export([ load/0
        , unload/0
        ]).

-export([]).

-export([ init/1
        , on_insta_create/3
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

load() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:load(exproto, RegistryOptions, []).

unload() ->
    emqx_gateway_registry:unload(exproto).

init(_) ->
    GwState = #{},
    {ok, GwState}.


%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

start_grpc_server(InstaId, Options = #{bind := ListenOn}) ->
    Services = #{protos => [emqx_exproto_pb],
                 services => #{
                   'emqx.exproto.v1.ConnectionAdapter' => emqx_exproto_gsvr}
                },
    SvrOptions = case maps:to_list(maps:get(ssl, Options, #{})) of
                  [] -> [];
                  SslOpts ->
                      [{ssl_options, SslOpts}]
              end,
    _ = grpc:start_server(InstaId, ListenOn, Services, SvrOptions),
    io:format("Start ~s gRPC server on ~p successfully.~n",
               [InstaId, ListenOn]).

start_grpc_client_channel(InstaId, Options = #{address := UriStr}) ->
    UriMap = uri_string:parse(UriStr),
    Scheme = maps:get(scheme, UriMap),
    Host = maps:get(host, UriMap),
    Port = maps:get(port, UriMap),
    SvrAddr = lists:flatten(
                io_lib:format(
                  "~s://~s:~w", [Scheme, Host, Port])
               ),
    ClientOpts = case Scheme of
                     "https" ->
                         SslOpts = maps:to_list(maps:get(ssl, Options, #{})),
                         #{gun_opts =>
                           #{transport => ssl,
                             transport_opts => SslOpts}};
                     _ -> #{}
                 end,
    grpc_client_sup:create_channel_pool(InstaId, SvrAddr, ClientOpts).

on_insta_create(_Insta = #{ id := InstaId,
                            rawconf := RawConf
                          }, Ctx, _GwState) ->
    %% XXX: How to monitor it ?
    %% Start grpc client pool & client channel
    PoolName = pool_name(InstaId),
    PoolSize = emqx_vm:schedulers() * 2,
    {ok, _} = emqx_pool_sup:start_link(PoolName, hash, PoolSize,
                                       {emqx_exproto_gcli, start_link, []}),
    _ = start_grpc_client_channel(InstaId, maps:get(handler, RawConf)),

    %% XXX: How to monitor it ?
    _ = start_grpc_server(InstaId, maps:get(server, RawConf)),

    NRawConf = maps:without(
                 [server, handler],
                 RawConf#{pool_name => PoolName}
                ),
    Listeners = emqx_gateway_utils:normalize_rawconf(
                  NRawConf#{handler => InstaId}
                 ),
    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(InstaId, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _InstaState = #{ctx => Ctx}}.

on_insta_update(NewInsta, OldInsta, GwInstaState = #{ctx := Ctx}, GwState) ->
    InstaId = maps:get(id, NewInsta),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_insta_destroy(OldInsta, GwInstaState, GwState),
        on_insta_create(NewInsta, Ctx, GwState)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update exproto instance ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [InstaId, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_insta_destroy(_Insta = #{ id := InstaId,
                             rawconf := RawConf
                           }, _GwInstaState, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(InstaId, Lis)
    end, Listeners).

pool_name(InstaId) ->
    list_to_atom(lists:concat([InstaId, "_gcli_pool"])).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(InstaId, Ctx, {Type, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            io:format("Start exproto ~s:~s listener on ~s successfully.~n",
                      [InstaId, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to start exproto ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(InstaId, Type),
    NCfg = Cfg#{
             ctx => Ctx,
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

name(InstaId, Type) ->
    list_to_atom(lists:concat([InstaId, ":", Type])).

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

stop_listener(InstaId, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(InstaId, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> io:format("Stop exproto ~s:~s listener on ~s successfully.~n",
                        [InstaId, Type, ListenOnStr]);
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to stop exproto ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]
                     )
    end,
    StopRet.

stop_listener(InstaId, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(InstaId, Type),
    esockd:close(Name, ListenOn).
