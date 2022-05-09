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

-module(emqx_authz_redis).

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
    ?PH_CERT_CN_NAME,
    ?PH_CERT_SUBJECT,
    ?PH_PEERHOST,
    ?PH_CLIENTID,
    ?PH_USERNAME
]).

description() ->
    "AuthZ with Redis".

create(#{cmd := CmdStr} = Source) ->
    Cmd = tokens(CmdStr),
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    CmdTemplate = emqx_authz_utils:parse_deep(Cmd, ?PLACEHOLDERS),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_connector_redis, Source),
    Source#{annotations => #{id => ResourceId}, cmd_template => CmdTemplate}.

update(#{cmd := CmdStr} = Source) ->
    Cmd = tokens(CmdStr),
    CmdTemplate = emqx_authz_utils:parse_deep(Cmd, ?PLACEHOLDERS),
    case emqx_authz_utils:update_resource(emqx_connector_redis, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id}, cmd_template => CmdTemplate}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

authorize(
    Client,
    PubSub,
    Topic,
    #{
        cmd_template := CmdTemplate,
        annotations := #{id := ResourceID}
    }
) ->
    Cmd = emqx_authz_utils:render_deep(CmdTemplate, Client),
    case emqx_resource:query(ResourceID, {cmd, Cmd}) of
        {ok, []} ->
            nomatch;
        {ok, Rows} ->
            do_authorize(Client, PubSub, Topic, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_redis_error",
                reason => Reason,
                cmd => Cmd,
                resource_id => ResourceID
            }),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [TopicFilter, Action | Tail]) ->
    case
        emqx_authz_rule:match(
            Client,
            PubSub,
            Topic,
            emqx_authz_rule:compile({allow, all, Action, [TopicFilter]})
        )
    of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

tokens(Query) ->
    Tokens = binary:split(Query, <<" ">>, [global]),
    [Token || Token <- Tokens, size(Token) > 0].
