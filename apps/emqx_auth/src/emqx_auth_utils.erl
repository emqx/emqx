%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_utils).

-export([
    cached_simple_sync_query/4,
    cached_apply/3
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec cached_simple_sync_query(
    emqx_auth_cache:name(),
    emqx_auth_cache:cache_key(),
    emqx_resource:resource_id(),
    _Request :: term()
) -> term().
cached_simple_sync_query(CacheName, CacheKey, ResourceID, Query) ->
    cached_apply(CacheName, CacheKey, fun() ->
        emqx_resource:simple_sync_query(ResourceID, eval_query(Query))
    end).

-doc """
Apply `Fun` caching its result in the auth cache.
Error results (`{error, _}`) are not cached.
""".
-spec cached_apply(
    emqx_auth_cache:name(),
    emqx_auth_cache:cache_key(),
    fun(() -> term())
) -> term().
cached_apply(CacheName, CacheKey, Fun) ->
    emqx_auth_cache:with_cache(CacheName, CacheKey, fun() ->
        case Fun() of
            {error, _} = Error ->
                {nocache, Error};
            Result ->
                {cache, Result}
        end
    end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

eval_query(Query) when is_function(Query, 0) ->
    Query();
eval_query(Query) ->
    Query.
