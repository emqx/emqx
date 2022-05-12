%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_postgresql).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(emqx_authz).

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

-define(PLACEHOLDERS, [
    ?PH_USERNAME,
    ?PH_CLIENTID,
    ?PH_PEERHOST,
    ?PH_CERT_CN_NAME,
    ?PH_CERT_SUBJECT
]).

description() ->
    "AuthZ with PostgreSQL".

create(#{query := SQL0} = Source) ->
    {SQL, PlaceHolders} = emqx_authz_utils:parse_sql(SQL0, '$n', ?PLACEHOLDERS),
    ResourceID = emqx_authz_utils:make_resource_id(emqx_connector_pgsql),
    {ok, _Data} = emqx_authz_utils:create_resource(
        ResourceID,
        emqx_connector_pgsql,
        Source#{prepare_statement => #{ResourceID => SQL}}
    ),
    Source#{annotations => #{id => ResourceID, placeholders => PlaceHolders}}.

update(#{query := SQL0, annotations := #{id := ResourceID}} = Source) ->
    {SQL, PlaceHolders} = emqx_authz_utils:parse_sql(SQL0, '$n', ?PLACEHOLDERS),
    case
        emqx_authz_utils:update_resource(
            emqx_connector_pgsql,
            Source#{prepare_statement => #{ResourceID => SQL}}
        )
    of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id, placeholders => PlaceHolders}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

authorize(
    Client,
    PubSub,
    Topic,
    #{
        annotations := #{
            id := ResourceID,
            placeholders := Placeholders
        }
    }
) ->
    RenderedParams = emqx_authz_utils:render_sql_params(Placeholders, Client),
    case emqx_resource:query(ResourceID, {prepared_query, ResourceID, RenderedParams}) of
        {ok, _Columns, []} ->
            nomatch;
        {ok, Columns, Rows} ->
            do_authorize(Client, PubSub, Topic, Columns, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_postgresql_error",
                reason => Reason,
                params => RenderedParams,
                resource_id => ResourceID
            }),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, _Columns, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, Columns, [Row | Tail]) ->
    case
        emqx_authz_rule:match(
            Client,
            PubSub,
            Topic,
            emqx_authz_rule:compile(format_result(Columns, Row))
        )
    of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Columns, Tail)
    end.

format_result(Columns, Row) ->
    Permission = lists:nth(index(<<"permission">>, 2, Columns), erlang:tuple_to_list(Row)),
    Action = lists:nth(index(<<"action">>, 2, Columns), erlang:tuple_to_list(Row)),
    Topic = lists:nth(index(<<"topic">>, 2, Columns), erlang:tuple_to_list(Row)),
    {Permission, all, Action, [Topic]}.

index(Key, N, TupleList) when is_integer(N) ->
    Tuple = lists:keyfind(Key, N, TupleList),
    index(Tuple, TupleList, 1);
index(_Tuple, [], _Index) ->
    {error, not_found};
index(Tuple, [Tuple | _TupleList], Index) ->
    Index;
index(Tuple, [_ | TupleList], Index) ->
    index(Tuple, TupleList, Index + 1).
