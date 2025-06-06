%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb).

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/1,
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

create(#{filter := Filter, skip := Skip, limit := Limit} = Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_mongodb, Source, ?AUTHZ_TYPE),
    {Vars, FilterTemp} = emqx_auth_template:parse_deep(
        emqx_utils_maps:binary_key_map(Filter), ?ALLOWED_VARS
    ),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    Source#{
        annotations => #{
            id => ResourceId,
            skip => Skip,
            limit => Limit,
            filter_template => FilterTemp,
            cache_key_template => CacheKeyTemplate
        }
    }.

update(#{filter := Filter, skip := Skip, limit := Limit} = Source) ->
    {Vars, FilterTemp} = emqx_auth_template:parse_deep(
        emqx_utils_maps:binary_key_map(Filter), ?ALLOWED_VARS
    ),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    case emqx_authz_utils:update_resource(emqx_mongodb, Source, ?AUTHZ_TYPE) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{
                annotations => #{
                    id => Id,
                    skip => Skip,
                    limit => Limit,
                    filter_template => FilterTemp,
                    cache_key_template => CacheKeyTemplate
                }
            }
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{annotations := #{filter_template := FilterTemplate}} = Config
) ->
    try emqx_auth_template:render_deep_for_json(FilterTemplate, Client) of
        RenderedFilter ->
            authorize_with_filter(RenderedFilter, Client, Action, Topic, Config)
    catch
        error:{encode_error, _} = EncodeError ->
            ?SLOG(error, #{
                msg => "mongo_authorize_error",
                reason => EncodeError
            }),
            nomatch
    end.

authorize_with_filter(RenderedFilter, Client, Action, Topic, #{
    collection := Collection,
    annotations := #{
        skip := Skip, limit := Limit, id := ResourceId, cache_key_template := CacheKeyTemplate
    }
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
