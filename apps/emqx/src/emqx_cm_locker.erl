%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_locker).

-include("emqx.hrl").
-include("types.hrl").

-export([start_link/0]).

-export([
    trans/2,
    lock/1,
    unlock/1
]).

-spec start_link() -> startlink_ret().
start_link() ->
    ekka_locker:start_link(?MODULE).

-spec trans(
    option(emqx_types:clientid()),
    fun(([node()]) -> any())
) -> any().
trans(undefined, Fun) ->
    Fun([]);
trans(ClientId, Fun) ->
    case lock(ClientId) of
        {true, Nodes} ->
            try
                Fun(Nodes)
            after
                unlock(ClientId)
            end;
        {false, _Nodes} ->
            {error, client_id_unavailable}
    end.

-spec lock(emqx_types:clientid()) -> {boolean(), [node() | {node(), any()}]}.
lock(ClientId) ->
    ekka_locker:acquire(?MODULE, ClientId, strategy()).

-spec unlock(emqx_types:clientid()) -> {boolean(), [node()]}.
unlock(ClientId) ->
    ekka_locker:release(?MODULE, ClientId, strategy()).

-spec strategy() -> local | leader | quorum | all.
strategy() ->
    emqx:get_config([broker, session_locking_strategy]).
