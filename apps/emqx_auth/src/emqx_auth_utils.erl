%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_utils).

-export([
    cached_simple_sync_query/4
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
    emqx_auth_cache:with_cache(CacheName, CacheKey, fun() ->
        case emqx_resource:simple_sync_query(ResourceID, eval_query(Query)) of
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
