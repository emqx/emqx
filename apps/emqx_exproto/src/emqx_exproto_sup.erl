%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ServerOpts = #{},
    GrpcOpts = #{service_protos => [emqx_exproto_pb],
                 services => #{'emqx.exproto.v1.ConnectionAdapter' => emqx_exproto_gsvr}},
    ListenOpts = #{port => Port, socket_options => [{reuseaddr, true}]},
    PoolOpts = #{size => 8},
    TransportOpts = maps:from_list(SSLOptions),
    Spec = #{id => Name,
             start => {grpcbox_services_sup, start_link,
                       [ServerOpts, GrpcOpts, ListenOpts,
                        PoolOpts, TransportOpts]},
             type => supervisor,
             restart => permanent,
             shutdown => infinity},
    supervisor:start_child(?MODULE, Spec).

-spec stop_grpc_server(atom()) -> ok.
stop_grpc_server(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name),
    ok = supervisor:delete_child(?MODULE, Name).

-spec start_grpc_client_channel(
        atom(),
        [grpcbox_channel:endpoint()],
        grpcbox_channel:options()) -> {ok, pid()} | {error, term()}.
start_grpc_client_channel(Name, Endpoints, Options0) ->
    Options = Options0#{sync_start => true},
    Spec = #{id => Name,
             start => {grpcbox_channel, start_link, [Name, Endpoints, Options]},
             type => worker},
    supervisor:start_child(?MODULE, Spec).

-spec stop_grpc_client_channel(atom()) -> ok.
stop_grpc_client_channel(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name),
    ok = supervisor:delete_child(?MODULE, Name).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% gRPC Client Pool
    PoolSize = emqx_vm:schedulers() * 2,
    Pool = emqx_pool_sup:spec([exproto_gcli_pool, hash, PoolSize,
                               {emqx_exproto_gcli, start_link, []}]),
    {ok, {{one_for_one, 10, 5}, [Pool]}}.
