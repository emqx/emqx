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

-module(emqx_exproto_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([ start_grpc_server/3
        , stop_grpc_server/1
        , start_grpc_client_channel/3
        , stop_grpc_client_channel/1
        ]).

-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_grpc_server(atom(), inet:port_number(), list())
  -> {ok, pid()} | {error, term()}.
start_grpc_server(Name, Port, SSLOptions) ->
    Services = #{protos => [emqx_exproto_pb],
                 services => #{'emqx.exproto.v1.ConnectionAdapter' => emqx_exproto_gsvr}
                },
    Options = case SSLOptions of
                  [] -> [];
                  _ ->
                      [{ssl_options, lists:keydelete(ssl, 1, SSLOptions)}]
              end,
    grpc:start_server(prefix(Name), Port, Services, Options).

-spec stop_grpc_server(atom()) -> ok.
stop_grpc_server(Name) ->
    grpc:stop_server(prefix(Name)).

-spec start_grpc_client_channel(
        atom(),
        uri_string:uri_string(),
        grpc_client:grpc_opts()) -> {ok, pid()} | {error, term()}.
start_grpc_client_channel(Name, SvrAddr, ClientOpts) ->
    grpc_client_sup:create_channel_pool(Name, SvrAddr, ClientOpts).

-spec stop_grpc_client_channel(atom()) -> ok.
stop_grpc_client_channel(Name) ->
    grpc_client_sup:stop_channel_pool(Name).

%% @private
prefix(Name) when is_atom(Name) ->
    "exproto:" ++ atom_to_list(Name);
prefix(Name) when is_binary(Name) ->
    "exproto:" ++ binary_to_list(Name);
prefix(Name) when is_list(Name) ->
    "exproto:" ++ Name.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% gRPC Client Pool
    PoolSize = emqx_vm:schedulers() * 2,
    Pool = emqx_pool_sup:spec([exproto_gcli_pool, hash, PoolSize,
                               {emqx_exproto_gcli, start_link, []}]),
    {ok, {{one_for_one, 10, 5}, [Pool]}}.
