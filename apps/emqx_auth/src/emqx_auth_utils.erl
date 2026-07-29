%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_utils).

-export([
    cached_simple_sync_query/4,
    sanitize_oauth2_ssl/1
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

-spec sanitize_oauth2_ssl(map()) -> map().
sanitize_oauth2_ssl(#{<<"ssl">> := SSL} = OAuth2) ->
    %% Empty certificate fields are optional when peer verification is disabled.
    OAuth2#{<<"ssl">> := drop_blank_certs_for_verify_none(SSL)};
sanitize_oauth2_ssl(OAuth2) ->
    OAuth2.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

drop_blank_certs_for_verify_none(#{<<"verify">> := Verify} = SSL) when
    Verify =:= verify_none; Verify =:= <<"verify_none">>
->
    maps:filter(
        fun(Key, Value) ->
            not (lists:member(Key, [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>]) andalso
                (Value =:= <<>> orelse Value =:= ""))
        end,
        SSL
    );
drop_blank_certs_for_verify_none(SSL) ->
    SSL.

eval_query(Query) when is_function(Query, 0) ->
    Query();
eval_query(Query) ->
    Query.
