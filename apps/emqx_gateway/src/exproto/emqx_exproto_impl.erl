%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").

-import(
    emqx_gateway_utils,
    [
        normalize_config/1,
        start_listeners/4,
        stop_listeners/2
    ]
).

%% APIs
-export([
    reg/0,
    unreg/0
]).

-export([
    on_gateway_load/2,
    on_gateway_update/3,
    on_gateway_unload/2
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

reg() ->
    RegistryOptions = [{cbkmod, ?MODULE}],
    emqx_gateway_registry:reg(exproto, RegistryOptions).

unreg() ->
    emqx_gateway_registry:unreg(exproto).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    Ctx
) ->
    %% XXX: How to monitor it ?
    _ = start_grpc_client_channel(
        GwName,
        maps:get(handler, Config, undefined)
    ),
    %% XXX: How to monitor it ?
    _ = start_grpc_server(GwName, maps:get(server, Config, undefined)),

    %% XXX: How to monitor it ?
    PoolName = pool_name(GwName),
    PoolSize = emqx_vm:schedulers() * 2,
    {ok, PoolSup} = emqx_pool_sup:start_link(
        PoolName,
        hash,
        PoolSize,
        {emqx_exproto_gcli, start_link, []}
    ),

    NConfig = maps:without(
        [server, handler],
        Config#{pool_name => PoolName}
    ),
    Listeners = emqx_gateway_utils:normalize_config(
        NConfig#{handler => GwName}
    ),

    ModCfg = #{
        frame_mod => emqx_exproto_frame,
        chann_mod => emqx_exproto_channel
    },
    case
        start_listeners(
            Listeners, GwName, Ctx, ModCfg
        )
    of
        {ok, ListenerPids} ->
            {ok, ListenerPids, _GwState = #{ctx => Ctx, pool => PoolSup}};
        {error, {Reason, Listener}} ->
            throw(
                {badconf, #{
                    key => listeners,
                    value => Listener,
                    reason => Reason
                }}
            )
    end.

on_gateway_update(Config, Gateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(Gateway, GwState),
        on_gateway_load(Gateway#{config => Config}, Ctx)
    catch
        Class:Reason:Stk ->
            logger:error(
                "Failed to update ~ts; "
                "reason: {~0p, ~0p} stacktrace: ~0p",
                [GwName, Class, Reason, Stk]
            ),
            {error, Reason}
    end.

on_gateway_unload(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    _GwState = #{pool := PoolSup}
) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    %% Stop funcs???
    exit(PoolSup, kill),
    stop_grpc_server(GwName),
    stop_grpc_client_channel(GwName),
    stop_listeners(GwName, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_grpc_server(_GwName, undefined) ->
    undefined;
start_grpc_server(GwName, Options = #{bind := ListenOn}) ->
    Services = #{
        protos => [emqx_exproto_pb],
        services => #{
            'emqx.exproto.v1.ConnectionAdapter' => emqx_exproto_gsvr
        }
    },
    SvrOptions =
        case emqx_map_lib:deep_get([ssl, enable], Options, false) of
            false ->
                [];
            true ->
                [
                    {ssl_options,
                        maps:to_list(
                            maps:without([enable, handshake_timeout], maps:get(ssl, Options, #{}))
                        )}
                ]
        end,
    ListenOnStr = emqx_listeners:format_addr(ListenOn),
    case grpc:start_server(GwName, ListenOn, Services, SvrOptions) of
        {ok, _SvrPid} ->
            console_print(
                "Start ~ts gRPC server on ~s successfully.~n",
                [GwName, ListenOnStr]
            );
        {error, Reason} ->
            ?ELOG(
                "Failed to start ~ts gRPC server on ~s, reason: ~0p",
                [GwName, ListenOnStr, Reason]
            ),
            throw(
                {badconf, #{
                    key => server,
                    value => Options,
                    reason => illegal_grpc_server_confs
                }}
            )
    end.

stop_grpc_server(GwName) ->
    _ = grpc:stop_server(GwName),
    console_print("Stop ~s gRPC server successfully.~n", [GwName]).

start_grpc_client_channel(_GwName, undefined) ->
    undefined;
start_grpc_client_channel(GwName, Options = #{address := Address}) ->
    #{host := Host, port := Port} =
        case emqx_http_lib:uri_parse(Address) of
            {ok, URIMap0} ->
                URIMap0;
            {error, _Reason} ->
                throw(
                    {badconf, #{
                        key => address,
                        value => Address,
                        reason => illegal_grpc_address
                    }}
                )
        end,
    case emqx_map_lib:deep_get([ssl, enable], Options, false) of
        false ->
            SvrAddr = compose_http_uri(http, Host, Port),
            grpc_client_sup:create_channel_pool(GwName, SvrAddr, #{});
        true ->
            SslOpts = maps:to_list(maps:get(ssl, Options, #{})),
            ClientOpts = #{
                gun_opts =>
                    #{
                        transport => ssl,
                        transport_opts => SslOpts
                    }
            },

            SvrAddr = compose_http_uri(https, Host, Port),
            grpc_client_sup:create_channel_pool(GwName, SvrAddr, ClientOpts)
    end.

compose_http_uri(Scheme, Host, Port) ->
    lists:flatten(
        io_lib:format(
            "~s://~s:~w", [Scheme, inet:ntoa(Host), Port]
        )
    ).

stop_grpc_client_channel(GwName) ->
    _ = grpc_client_sup:stop_channel_pool(GwName),
    ok.

pool_name(GwName) ->
    list_to_atom(lists:concat([GwName, "_gcli_pool"])).

-ifndef(TEST).
console_print(Fmt, Args) -> ?ULOG(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.
