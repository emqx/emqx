%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_http_pool_clients).

-export([
    create_table/0,

    register/4,
    unregister/2
]).

-define(TAB, ?MODULE).

-spec create_table() -> ok.
create_table() ->
    ets:new(?TAB, [
        private,
        set
    ]).

-spec register(ets:tid(), pid(), reference(), emqx_s3_profile_http_pools:pool_name()) -> ok.
register(Tab, Pid, MRef, PoolName) ->
    true = ets:insert(Tab, {Pid, {MRef, PoolName}}),
    ok.

-spec unregister(ets:tid(), pid()) ->
    {reference(), emqx_s3_profile_http_pools:pool_name()} | undefined.
unregister(Tab, Pid) ->
    case ets:take(Tab, Pid) of
        [{Pid, {MRef, PoolName}}] ->
            {MRef, PoolName};
        [] ->
            undefined
    end.
