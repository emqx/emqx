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

-module(emqx_authz_mysql).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(emqx_authz).

-define(PREPARE_KEY, ?MODULE).

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
    "AuthZ with Mysql".

create(#{query := SQL} = Source0) ->
    {PrepareSQL, TmplToken} = emqx_authz_utils:parse_sql(SQL, '?', ?PLACEHOLDERS),
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_connector_mysql, Source),
    Source#{annotations => #{id => ResourceId, tmpl_oken => TmplToken}}.

update(#{query := SQL} = Source0) ->
    {PrepareSQL, TmplToken} = emqx_authz_utils:parse_sql(SQL, '?', ?PLACEHOLDERS),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    case emqx_authz_utils:update_resource(emqx_connector_mysql, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id, tmpl_oken => TmplToken}}
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
            tmpl_oken := TmplToken
        }
    }
) ->
    RenderParams = emqx_authz_utils:render_sql_params(TmplToken, Client),
    case emqx_resource:query(ResourceID, {prepared_query, ?PREPARE_KEY, RenderParams}) of
        {ok, _Columns, []} ->
            nomatch;
        {ok, Columns, Rows} ->
            do_authorize(Client, PubSub, Topic, Columns, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mysql_error",
                reason => Reason,
                tmpl_oken => TmplToken,
                params => RenderParams,
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
    Permission = lists:nth(index(<<"permission">>, Columns), Row),
    Action = lists:nth(index(<<"action">>, Columns), Row),
    Topic = lists:nth(index(<<"topic">>, Columns), Row),
    {Permission, all, Action, [Topic]}.

index(Elem, List) ->
    index(Elem, List, 1).
index(_Elem, [], _Index) -> {error, not_found};
index(Elem, [Elem | _List], Index) -> Index;
index(Elem, [_ | List], Index) -> index(Elem, List, Index + 1).
