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

%% @doc The instance running context
-type context() ::
        #{ instid  := instance_id()
         , gwid    := gateway_type()
         , auth    := allow_anonymouse | emqx_authentication:chain_id()
         , cm      := pid()
         %%, metrics := metrics()
         %% authenticators?
         %% clientinfo_override
         %%
         %% hooks   ?
         %% pubsub  ? acl ?
         }.

-export([ authenticate/2
        , open_session/5
        , set_chann_info/3
        , set_chann_stats/3
        %, get_chann_info/0     %% TODO:
        %, get_chann_stat/0
        ]).

-export([ publish/3
        , subscribe/4
        ]).

%-export([ recv/3
%        , send/3
%        ]).

%% Connect&Auth circle

-spec authenticate(context(), emqx_types:clientinfo())
    -> {ok, emqx_types:clientinfo()}
     | {error, any()}.
authenticate(_Ctx = #{auth := allow_anonymouse}, ClientInfo) ->
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

%% Session circle

%% @doc Register the session to the cluster.
%%      This function should be called after the client has authenticated
%%      successfully so that the client can be managed in the cluster.
%%

%% 如果没有 Session 如何在集群中保持唯一?
%%  OpenSession ??
%%
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

open_session(_Ctx = #{gwid := GwId},
             CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    emqx_gateway_cm:open_session(GwId, CleanStart,
                                 ClientInfo, ConnInfo, CreateSessionFun).

-spec set_chann_info(context(), binary(), map()) -> boolean().
set_chann_info(_Ctx = #{gwid := GwId}, ClientId, Info) ->
    emqx_gateway_cm:set_chann_info(GwId, ClientId, Info).

-spec set_chann_stats(context(), binary(), map()) -> boolean().
set_chann_stats(_Ctx = #{gwid := GwId}, ClientId, Stats) ->
    emqx_gateway_cm:set_chann_stats(GwId, ClientId, Stats).

%% TODO.
-spec publish(context(), any(), emqx_types:clientinfo()) -> ok.
%% 1. ACL Checking
%% 2. Pub Limit, Quota chekcing
%% 3. Fire hooks ( message.publish, message.dropped )?
%% 4.
publish(_Ctx, _Msg, _ClientInfo) ->
    todo.

-spec subscribe(context(), binary(), integer(), emqx_types:clientinfo()) -> ok.
%% 1. ACL Checking
%% 2. Sub Limit, Quota checking
%% 3. Fire hooks ( client.subscribe, session.subscribed )
%% 4.
subscribe(_Ctx, _Topic, _Qos, _ClientInfo) ->
    todo.

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
