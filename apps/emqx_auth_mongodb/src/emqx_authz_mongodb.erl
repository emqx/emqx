%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

description() ->
    "AuthZ with MongoDB".

create(#{filter := Filter} = Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_mongodb, Source),
    {Vars, FilterTemp} = emqx_auth_template:parse_deep(Filter, ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_utils:cache_key_template(Vars),
    Source#{
        annotations => #{id => ResourceId, cache_key_template => CacheKeyTemplate},
        filter_template => FilterTemp
    }.

update(#{filter := Filter} = Source) ->
    {Vars, FilterTemp} = emqx_auth_template:parse_deep(Filter, ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_utils:cache_key_template(Vars),
    case emqx_authz_utils:update_resource(emqx_mongodb, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{
                annotations => #{id => Id},
                filter_template => FilterTemp,
                cache_key_template => CacheKeyTemplate
            }
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{filter_template := FilterTemplate} = Config
) ->
    try emqx_auth_template:render_deep_for_json(FilterTemplate, Client) of
        RenderedFilter -> authorize_with_filter(RenderedFilter, Client, Action, Topic, Config)
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
    annotations := #{id := ResourceID, cache_key_template := CacheKeyTemplate}
}) ->
    CacheKey = emqx_auth_utils:cache_key(Client, CacheKeyTemplate, {ResourceID, Collection}),
    Result = emqx_authz_utils:cached_simple_sync_query(
        CacheKey, ResourceID, {find, Collection, RenderedFilter, #{}}
    ),
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mongo_error",
                reason => Reason,
                collection => Collection,
                filter => RenderedFilter,
                resource_id => ResourceID
            }),
            nomatch;
        {ok, Rows} ->
            Rules = lists:flatmap(fun parse_rule/1, Rows),
            do_authorize(Client, Action, Topic, Rules)
    end.

parse_rule(Row) ->
    case emqx_authz_rule_raw:parse_rule(Row) of
        {ok, {Permission, Action, Topics}} ->
            [emqx_authz_rule:compile({Permission, all, Action, Topics})];
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
