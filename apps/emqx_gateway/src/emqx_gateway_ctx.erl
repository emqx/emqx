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

%% @doc The gateway instance context
-module(emqx_gateway_ctx).

-include("include/emqx_gateway.hrl").

-logger_header(["PGW-Ctx"]).

%% @doc The running context for a Connection/Channel process.
%%
%% The `Context` encapsulates a complex structure of contextual information.
%% It is convenient to use it directly in Channel/Connection to read
%% configuration, register devices and other common operations.
%%
-type context() ::
        #{ %% Gateway Instance ID
           instid := instance_id()
           %% Gateway ID
         , type   := gateway_type()
           %% Autenticator
         , auth   := allow_anonymous | emqx_authentication:chain_id()
           %% The ConnectionManager PID
         , cm     := pid()
         }.

%% Authentication circle
-export([ authenticate/2
        , open_session/5
        , insert_channel_info/4
        , set_chan_info/3
        , set_chan_stats/3
        , connection_closed/2
        ]).

%% Message circle
-export([ authorize/4
        % Needless for pub/sub
        %, publish/3
        %, subscribe/4
        ]).

%% Metrics & Stats
-export([ metrics_inc/2
        , metrics_inc/3
        ]).

%%--------------------------------------------------------------------
%% Authentication circle

%% @doc Authenticate whether the client has access to the Broker.
-spec authenticate(context(), emqx_types:clientinfo())
    -> {ok, emqx_types:clientinfo()}
     | {error, any()}.
authenticate(_Ctx = #{auth := allow_anonymous}, ClientInfo) ->
    {ok, ClientInfo#{anonymous => true}};
authenticate(_Ctx = #{auth := ChainId}, ClientInfo0) ->
    ClientInfo = ClientInfo0#{
                   zone => undefined,
                   chain_id => ChainId
                  },
    case emqx_access_control:authenticate(ClientInfo) of
        {ok, AuthResult} ->
            {ok, mountpoint(maps:merge(ClientInfo, AuthResult))};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Register the session to the cluster.
%%
%%  This function should be called after the client has authenticated
%%  successfully so that the client can be managed in the cluster.
-spec open_session(context(), boolean(), emqx_types:clientinfo(),
                   emqx_types:conninfo(), function())
    -> {ok, #{session := any(),
              present := boolean(),
              pendings => list()
             }}
     | {error, any()}.
open_session(Ctx, false, ClientInfo, ConnInfo, CreateSessionFun) ->
    logger:warning("clean_start=false is not supported now, "
                   "fallback to clean_start mode"),
    open_session(Ctx, true, ClientInfo, ConnInfo, CreateSessionFun);

open_session(_Ctx = #{type := Type},
             CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    emqx_gateway_cm:open_session(Type, CleanStart,
                                 ClientInfo, ConnInfo, CreateSessionFun).

-spec insert_channel_info(context(),
                          emqx_types:clientid(),
                          emqx_types:infos(),
                          emqx_types:stats()) -> ok.
insert_channel_info(_Ctx = #{type := Type}, ClientId, Infos, Stats) ->
    emqx_gateway_cm:insert_channel_info(Type, ClientId, Infos, Stats).

%% @doc Set the Channel Info to the ConnectionManager for this client
-spec set_chan_info(context(),
                    emqx_types:clientid(),
                    emqx_types:infos()) -> boolean().
set_chan_info(_Ctx = #{type := Type}, ClientId, Infos) ->
    emqx_gateway_cm:set_chan_info(Type, ClientId, Infos).

-spec set_chan_stats(context(),
                     emqx_types:clientid(),
                     emqx_types:stats()) -> boolean().
set_chan_stats(_Ctx = #{type := Type}, ClientId, Stats) ->
    emqx_gateway_cm:set_chan_stats(Type, ClientId, Stats).

-spec connection_closed(context(), emqx_types:clientid()) -> boolean().
connection_closed(_Ctx = #{type := Type}, ClientId) ->
    emqx_gateway_cm:connection_closed(Type, ClientId).

-spec authorize(context(), emqx_types:clientinfo(),
                emqx_types:pubsub(), emqx_types:topic())
    -> allow | deny.
authorize(_Ctx, ClientInfo, PubSub, Topic) ->
    emqx_access_control:authorize(ClientInfo, PubSub, Topic).

metrics_inc(_Ctx = #{type := Type}, Name) ->
    emqx_gateway_metrics:inc(Type, Name).

metrics_inc(_Ctx = #{type := Type}, Name, Oct) ->
    emqx_gateway_metrics:inc(Type, Name, Oct).

%% Client Management Infos
%%
%% 0. Handle Deliverys
%%      - Msg Queue
%%      - Delivery Strategy
%%      - Inflight
%%
%% 是否可以考虑实现一个 emqx_gateway_protocol:handle_info/1 的方法:
%% 1. 用于封装这些 API 的处理
%%
%%
%% 1. API Management
%%      - Establish subscription
%%      - Kickout
%%
%% 2. Ratelimit
%%

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

mountpoint(ClientInfo = #{mountpoint := undefined}) ->
    ClientInfo;
mountpoint(ClientInfo = #{mountpoint := MountPoint}) ->
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    ClientInfo#{mountpoint := MountPoint1}.
