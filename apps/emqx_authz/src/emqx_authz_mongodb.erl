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

-module(emqx_authz_mongodb).

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
    "AuthZ with MongoDB".

create(#{filter := Filter} = Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_connector_mongo, Source),
    FilterTemp = emqx_authz_utils:parse_deep(Filter, ?PLACEHOLDERS),
    Source#{annotations => #{id => ResourceId}, filter_template => FilterTemp}.

update(#{filter := Filter} = Source) ->
    FilterTemp = emqx_authz_utils:parse_deep(Filter, ?PLACEHOLDERS),
    case emqx_authz_utils:update_resource(emqx_connector_mongo, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id}, filter_template => FilterTemp}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

authorize(
    Client,
    PubSub,
    Topic,
    #{
        collection := Collection,
        filter_template := FilterTemplate,
        annotations := #{id := ResourceID}
    }
) ->
    RenderedFilter = emqx_authz_utils:render_deep(FilterTemplate, Client),
    Result =
        try
            emqx_resource:query(ResourceID, {find, Collection, RenderedFilter, #{}})
        catch
            error:Error -> {error, Error}
        end,

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
        {ok, []} ->
            nomatch;
        {ok, Rows} ->
            Rules = [
                emqx_authz_rule:compile({Permission, all, Action, Topics})
             || #{
                    <<"topics">> := Topics,
                    <<"permission">> := Permission,
                    <<"action">> := Action
                } <- Rows
            ],
            do_authorize(Client, PubSub, Topic, Rules)
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    case emqx_authz_rule:match(Client, PubSub, Topic, Rule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.
