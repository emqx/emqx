%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_http_pools).

-include_lib("stdlib/include/ms_transform.hrl").

-export([
    create_table/0,

    register/2,
    unregister/2,

    register_client/2,
    unregister_client/2,

    all/1
]).

-export_type([pool_name/0]).

-define(TAB, ?MODULE).

-type pool_name() :: ecpool:pool_name().

-type pool_key() :: {emqx_s3:profile_id(), pool_name()}.

-record(pool, {
    key :: pool_key(),
    client_count = 0 :: integer(),
    deadline = undefined :: undefined | integer(),
    extra = #{} :: map()
}).

-spec create_table() -> ok.
create_table() ->
    _ = ets:new(?TAB, [
        named_table,
        public,
        ordered_set,
        {keypos, #pool.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ok.

-spec register(emqx_s3:profile_id(), pool_name()) ->
    ok.
register(ProfileId, PoolName) ->
    Key = key(ProfileId, PoolName),
    true = ets:insert(?TAB, #pool{
        key = Key,
        client_count = 0,
        deadline = undefined,
        extra = #{}
    }),
    ok.

-spec unregister(emqx_s3:profile_id(), pool_name()) ->
    ok.
unregister(ProfileId, PoolName) ->
    Key = key(ProfileId, PoolName),
    true = ets:delete(?TAB, Key),
    ok.

-spec register_client(emqx_s3:profile_id(), pool_name()) ->
    integer().
register_client(ProfileId, PoolName) ->
    Key = key(ProfileId, PoolName),
    ets:update_counter(?TAB, Key, {#pool.client_count, 1}).

-spec unregister_client(emqx_s3:profile_id(), pool_name()) ->
    integer().
unregister_client(ProfileId, PoolName) ->
    Key = key(ProfileId, PoolName),
    try
        ets:update_counter(?TAB, Key, {#pool.client_count, -1})
    catch
        error:badarg ->
            undefined
    end.

-spec all(emqx_s3:profile_id()) ->
    [pool_name()].
all(ProfileId) ->
    MS = ets:fun2ms(
        fun(#pool{key = {CurProfileId, CurPoolName}}) when CurProfileId =:= ProfileId ->
            CurPoolName
        end
    ),
    ets:select(?TAB, MS).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

key(ProfileId, PoolName) ->
    {ProfileId, PoolName}.
