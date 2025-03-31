%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mysql).

-behaviour(emqx_authz_source).

-define(PREPARE_KEY, ?MODULE).

%% AuthZ Callbacks
-export([
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include("emqx_auth_mysql.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

create(#{query := SQL} = Source0) ->
    {Vars, PrepareSQL, TmplToken} = emqx_auth_template:parse_sql(SQL, '?', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_mysql, Source, ?AUTHZ_TYPE),
    Source#{
        annotations => #{
            id => ResourceId, tmpl_token => TmplToken, cache_key_template => CacheKeyTemplate
        }
    }.

update(#{query := SQL} = Source0) ->
    {Vars, PrepareSQL, TmplToken} = emqx_auth_template:parse_sql(SQL, '?', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    case emqx_authz_utils:update_resource(emqx_mysql, Source, ?AUTHZ_TYPE) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{
                annotations => #{
                    id => Id, tmpl_token => TmplToken, cache_key_template => CacheKeyTemplate
                }
            }
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        annotations := #{
            id := ResourceId,
            tmpl_token := TmplToken,
            cache_key_template := CacheKeyTemplate
        }
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    RenderParams = emqx_auth_template:render_sql_params(TmplToken, Vars),
    CacheKey = emqx_auth_template:cache_key(Vars, CacheKeyTemplate),
    case
        emqx_authz_utils:cached_simple_sync_query(
            CacheKey, ResourceId, {prepared_query, ?PREPARE_KEY, RenderParams}
        )
    of
        {ok, ColumnNames, Rows} ->
            do_authorize(Client, Action, Topic, ColumnNames, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mysql_error",
                reason => Reason,
                tmpl_token => TmplToken,
                params => RenderParams,
                resource_id => ResourceId
            }),
            nomatch
    end.

do_authorize(_Client, _Action, _Topic, _ColumnNames, []) ->
    nomatch;
do_authorize(Client, Action, Topic, ColumnNames, [Row | Tail]) ->
    case emqx_authz_utils:do_authorize(mysql, Client, Action, Topic, ColumnNames, Row) of
        nomatch ->
            do_authorize(Client, Action, Topic, ColumnNames, Tail);
        {matched, Permission} ->
            {matched, Permission}
    end.
