%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_redis).

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
-include("emqx_auth_redis.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:create_resource(emqx_redis, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_redis, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{
        resource_id := ResourceId,
        cmd_template := CmdTemplate,
        cache_key_template := CacheKeyTemplate
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    Cmd = emqx_auth_template:render_deep_for_raw(CmdTemplate, Vars),
    CacheKey = emqx_auth_template:cache_key(Vars, CacheKeyTemplate),
    case emqx_authz_utils:cached_simple_sync_query(CacheKey, ResourceId, {cmd, Cmd}) of
        {ok, Rows} ->
            do_authorize(Client, Action, Topic, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_redis_error",
                reason => Reason,
                cmd => Cmd,
                resource_id => ResourceId
            }),
            nomatch
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_state(ResourceId, #{cmd := CmdStr} = Source) ->
    {Vars, CmdTemplate} = parse_cmd(CmdStr),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    ResourceConfig = emqx_authz_utils:resource_config(
        [cmd], Source
    ),
    emqx_authz_utils:init_state(Source, #{
        resource_config => ResourceConfig,
        resource_id => ResourceId,
        cmd_template => CmdTemplate,
        cache_key_template => CacheKeyTemplate
    }).

do_authorize(_Client, _Action, _Topic, []) ->
    nomatch;
do_authorize(Client, Action, Topic, [TopicFilterRaw, RuleEncoded | Tail]) ->
    case parse_rule(RuleEncoded) of
        {ok, RuleMap0} ->
            RuleMap =
                maps:merge(
                    #{
                        <<"permission">> => <<"allow">>,
                        <<"topic">> => TopicFilterRaw
                    },
                    RuleMap0
                ),
            case
                emqx_authz_utils:authorize_with_row(
                    redis, Client, Action, Topic, undefined, RuleMap
                )
            of
                nomatch ->
                    do_authorize(Client, Action, Topic, Tail);
                {matched, Permission} ->
                    {matched, Permission}
            end;
        {error, Reason} ->
            ?SLOG(error, Reason#{
                msg => "parse_rule_error",
                rule => RuleEncoded
            }),
            do_authorize(Client, Action, Topic, Tail)
    end.

parse_cmd(Query) ->
    case emqx_redis_command:split(Query) of
        {ok, Cmd} ->
            ok = validate_cmd(Cmd),
            emqx_auth_template:parse_deep(Cmd, ?ALLOWED_VARS);
        {error, Reason} ->
            error({invalid_redis_cmd, Reason, Query})
    end.

validate_cmd(Cmd) ->
    case
        emqx_auth_redis_validations:validate_command(
            [
                not_empty,
                {command_name, [<<"hmget">>, <<"hgetall">>]}
            ],
            Cmd
        )
    of
        ok -> ok;
        {error, Reason} -> error({invalid_redis_cmd, Reason, Cmd})
    end.

parse_rule(<<"publish">>) ->
    {ok, #{<<"action">> => <<"publish">>}};
parse_rule(<<"subscribe">>) ->
    {ok, #{<<"action">> => <<"subscribe">>}};
parse_rule(<<"all">>) ->
    {ok, #{<<"action">> => <<"all">>}};
parse_rule(Bin) when is_binary(Bin) ->
    case emqx_utils_json:safe_decode(Bin) of
        {ok, Map} when is_map(Map) ->
            {ok, maps:with([<<"qos">>, <<"action">>, <<"retain">>], Map)};
        {ok, _} ->
            {error, #{reason => invalid_topic_rule_not_map, value => Bin}};
        {error, _Error} ->
            {error, #{reason => invalid_topic_rule_not_json, value => Bin}}
    end.
