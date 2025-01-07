%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export_type([context/0]).

-include("emqx_gateway.hrl").

%% @doc The running context for a Connection/Channel process.
%%
%% The `Context` encapsulates a complex structure of contextual information.
%% It is convenient to use it directly in Channel/Connection to read
%% configuration, register devices and other common operations.
%%
-type context() ::
    #{
        %% Gateway Name
        gwname := gateway_name(),
        %% FIXME: use process name instead of pid()
        %% The ConnectionManager PID
        cm := pid()
    }.

%% Authentication circle
-export([
    authenticate/2,
    connection_expire_interval/2,
    open_session/5,
    open_session/6,
    insert_channel_info/4,
    set_chan_info/3,
    set_chan_stats/3,
    connection_closed/2
]).

%% Message circle
-export([
    authorize/4
    % Needless for pub/sub
    %, publish/3
    %, subscribe/4
]).

%% Metrics & Stats
-export([
    metrics_inc/2,
    metrics_inc/3
]).

%%--------------------------------------------------------------------
%% Authentication circle

%% @doc Authenticate whether the client has access to the Broker.
-spec authenticate(context(), emqx_types:clientinfo()) ->
    {ok, emqx_types:clientinfo()}
    | {error, any()}.
authenticate(_Ctx, ClientInfo0) ->
    ClientInfo = ClientInfo0#{zone => default},
    case emqx_access_control:authenticate(ClientInfo) of
        {ok, AuthResult} ->
            ClientInfo1 = merge_auth_result(ClientInfo, AuthResult),
            {ok, eval_mountpoint(ClientInfo1)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec connection_expire_interval(context(), emqx_types:clientinfo()) ->
    undefined | non_neg_integer().
connection_expire_interval(_Ctx, #{auth_expire_at := undefined}) ->
    undefined;
connection_expire_interval(_Ctx, #{auth_expire_at := ExpireAt}) ->
    max(0, ExpireAt - erlang:system_time(millisecond)).

%% @doc Register the session to the cluster.
%%
%%  This function should be called after the client has authenticated
%%  successfully so that the client can be managed in the cluster.
-spec open_session(
    context(),
    boolean(),
    emqx_types:clientinfo(),
    emqx_types:conninfo(),
    fun(
        (
            emqx_types:clientinfo(),
            emqx_types:conninfo()
        ) -> Session
    )
) ->
    {ok, #{
        session := Session,
        present := boolean(),
        pendings => list()
    }}
    | {error, any()}.
open_session(Ctx, CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    open_session(
        Ctx,
        CleanStart,
        ClientInfo,
        ConnInfo,
        CreateSessionFun,
        emqx_session
    ).

open_session(
    _Ctx = #{gwname := GwName},
    CleanStart,
    ClientInfo,
    ConnInfo,
    CreateSessionFun,
    SessionMod
) ->
    emqx_gateway_cm:open_session(
        GwName,
        CleanStart,
        ClientInfo,
        ConnInfo,
        CreateSessionFun,
        SessionMod
    ).

-spec insert_channel_info(
    context(),
    emqx_types:clientid(),
    emqx_types:infos(),
    emqx_types:stats()
) -> ok.
insert_channel_info(_Ctx = #{gwname := GwName}, ClientId, Infos, Stats) ->
    emqx_gateway_cm:insert_channel_info(GwName, ClientId, Infos, Stats).

%% @doc Set the Channel Info to the ConnectionManager for this client
-spec set_chan_info(
    context(),
    emqx_types:clientid(),
    emqx_types:infos()
) -> boolean().
set_chan_info(_Ctx = #{gwname := GwName}, ClientId, Infos) ->
    emqx_gateway_cm:set_chan_info(GwName, ClientId, Infos).

-spec set_chan_stats(
    context(),
    emqx_types:clientid(),
    emqx_types:stats()
) -> boolean().
set_chan_stats(_Ctx = #{gwname := GwName}, ClientId, Stats) ->
    emqx_gateway_cm:set_chan_stats(GwName, ClientId, Stats).

-spec connection_closed(context(), emqx_types:clientid()) -> boolean().
connection_closed(_Ctx = #{gwname := GwName}, ClientId) ->
    emqx_gateway_cm:connection_closed(GwName, ClientId).

%%--------------------------------------------------------------------
%% Message circle

-spec authorize(
    context(),
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic()
) ->
    allow | deny.
authorize(_Ctx, ClientInfo, Action, Topic) ->
    emqx_access_control:authorize(ClientInfo, Action, Topic).

%%--------------------------------------------------------------------
%% Metrics & Stats

metrics_inc(_Ctx = #{gwname := GwName}, Name) ->
    emqx_gateway_metrics:inc(GwName, Name).

metrics_inc(_Ctx = #{gwname := GwName}, Name, Oct) ->
    emqx_gateway_metrics:inc(GwName, Name, Oct).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

eval_mountpoint(ClientInfo = #{mountpoint := undefined}) ->
    ClientInfo;
eval_mountpoint(ClientInfo = #{mountpoint := MountPoint}) ->
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    ClientInfo#{mountpoint := MountPoint1}.

merge_auth_result(ClientInfo, AuthResult0) when is_map(ClientInfo) andalso is_map(AuthResult0) ->
    IsSuperuser = maps:get(is_superuser, AuthResult0, false),
    ExpireAt = maps:get(expire_at, AuthResult0, undefined),
    AuthResult1 = maps:without([expire_at], AuthResult0),
    maps:merge(ClientInfo#{auth_expire_at => ExpireAt}, AuthResult1#{is_superuser => IsSuperuser}).
