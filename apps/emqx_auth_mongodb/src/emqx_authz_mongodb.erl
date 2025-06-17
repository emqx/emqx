%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb).

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include("emqx_auth_mongodb.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:create_resource(emqx_mongodb, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_mongodb, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{filter_template := FilterTemplate} = State
) ->
    try emqx_auth_template:render_deep_for_json(FilterTemplate, Client) of
        RenderedFilter ->
            authorize_with_filter(RenderedFilter, Client, Action, Topic, State)
    catch
        error:{encode_error, _} = EncodeError ->
            ?SLOG(error, #{
                msg => "mongo_authorize_error",
                reason => EncodeError
            }),
            nomatch
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_state(
    ResourceId, #{filter := Filter, skip := Skip, limit := Limit, collection := Collection} = Source
) ->
    {Vars, FilterTemp} = emqx_auth_template:parse_deep(
        emqx_utils_maps:binary_key_map(Filter), ?ALLOWED_VARS
    ),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    ResourceConfig = emqx_authz_utils:cleanup_resource_config(
        [collection, skip, limit, filter], Source
    ),
    emqx_authz_utils:init_state(Source, #{
        resource_config => ResourceConfig,
        resource_id => ResourceId,
        collection => Collection,
        skip => Skip,
        limit => Limit,
        filter_template => FilterTemp,
        cache_key_template => CacheKeyTemplate
    }).

authorize_with_filter(RenderedFilter, Client, Action, Topic, #{
    collection := Collection,
    skip := Skip,
    limit := Limit,
    resource_id := ResourceId,
    cache_key_template := CacheKeyTemplate
}) ->
    Options = #{skip => Skip, limit => Limit},
    CacheKey = emqx_auth_template:cache_key(Client, CacheKeyTemplate),
    Result = emqx_authz_utils:cached_simple_sync_query(
        CacheKey, ResourceId, {find, Collection, RenderedFilter, Options}
    ),
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mongo_error",
                reason => Reason,
                collection => Collection,
                filter => RenderedFilter,
                options => Options,
                resource_id => ResourceId
            }),
            nomatch;
        {ok, Rows} ->
            Rules = lists:flatmap(fun parse_rule/1, Rows),
            do_authorize(Client, Action, Topic, Rules)
    end.

parse_rule(Row) ->
    case emqx_authz_rule_raw:parse_rule(Row) of
        {ok, {Permission, Who, Action, Topics}} ->
            [emqx_authz_rule:compile({Permission, Who, Action, Topics})];
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "parse_rule_error",
                reason => Reason,
                row => Row
            }),
            []
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    case emqx_authz_rule:match(Client, PubSub, Topic, Rule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.
