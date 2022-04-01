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

-module(emqx_authz_http).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(emqx_authz).

%% AuthZ Callbacks
-export([
    description/0,
    init/1,
    destroy/1,
    authorize/4,
    parse_url/1
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(PLACEHOLDERS, [
    ?PH_USERNAME,
    ?PH_CLIENTID,
    ?PH_PEERHOST,
    ?PH_PROTONAME,
    ?PH_MOUNTPOINT,
    ?PH_TOPIC,
    ?PH_ACTION
]).

description() ->
    "AuthZ with http".

init(Config) ->
    NConfig = parse_config(Config),
    case emqx_authz_utils:create_resource(emqx_connector_http, NConfig) of
        {error, Reason} -> error({load_config_error, Reason});
        {ok, Id} -> NConfig#{annotations => #{id => Id}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

authorize(
    Client,
    PubSub,
    Topic,
    #{
        type := http,
        annotations := #{id := ResourceID},
        method := Method,
        request_timeout := RequestTimeout
    } = Config
) ->
    Request = generate_request(PubSub, Topic, Client, Config),
    case emqx_resource:query(ResourceID, {Method, Request, RequestTimeout}) of
        {ok, 200, _Headers} ->
            {matched, allow};
        {ok, 204, _Headers} ->
            {matched, allow};
        {ok, 200, _Headers, _Body} ->
            {matched, allow};
        {ok, _Status, _Headers} ->
            nomatch;
        {ok, _Status, _Headers, _Body} ->
            nomatch;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_server_query_failed",
                resource => ResourceID,
                reason => Reason
            }),
            ignore
    end.

parse_config(
    #{
        url := URL,
        method := Method,
        headers := Headers,
        request_timeout := ReqTimeout
    } = Conf
) ->
    {BaseURLWithPath, Query} = parse_fullpath(URL),
    BaseURLMap = parse_url(BaseURLWithPath),
    Conf#{
        method => Method,
        base_url => maps:remove(query, BaseURLMap),
        base_query_template => emqx_authz_utils:parse_deep(
            cow_qs:parse_qs(bin(Query)),
            ?PLACEHOLDERS
        ),
        body_template => emqx_authz_utils:parse_deep(
            maps:to_list(maps:get(body, Conf, #{})),
            ?PLACEHOLDERS
        ),
        headers => Headers,
        request_timeout => ReqTimeout,
        %% pool_type default value `random`
        pool_type => random
    }.

parse_fullpath(RawURL) ->
    cow_http:parse_fullpath(bin(RawURL)).

parse_url(URL) when
    URL =:= undefined
->
    #{};
parse_url(URL) ->
    {ok, URIMap} = emqx_http_lib:uri_parse(URL),
    case maps:get(query, URIMap, undefined) of
        undefined ->
            URIMap#{query => ""};
        _ ->
            URIMap
    end.

generate_request(
    PubSub,
    Topic,
    Client,
    #{
        method := Method,
        base_url := #{path := Path},
        base_query_template := BaseQueryTemplate,
        headers := Headers,
        body_template := BodyTemplate
    }
) ->
    Values = client_vars(Client, PubSub, Topic),
    Body = emqx_authz_utils:render_deep(BodyTemplate, Values),
    NBaseQuery = emqx_authz_utils:render_deep(BaseQueryTemplate, Values),
    case Method of
        get ->
            NPath = append_query(Path, NBaseQuery ++ Body),
            {NPath, Headers};
        _ ->
            NPath = append_query(Path, NBaseQuery),
            NBody = serialize_body(
                proplists:get_value(<<"Accept">>, Headers, <<"application/json">>),
                Body
            ),
            {NPath, Headers, NBody}
    end.

append_query(Path, []) ->
    Path;
append_query(Path, Query) ->
    Path ++ "?" ++ binary_to_list(query_string(Query)).

query_string(Body) ->
    query_string(Body, []).

query_string([], Acc) ->
    case iolist_to_binary(lists:reverse(Acc)) of
        <<$&, Str/binary>> ->
            Str;
        <<>> ->
            <<>>
    end;
query_string([{K, V} | More], Acc) ->
    query_string(
        More,
        [
            ["&", emqx_http_lib:uri_encode(K), "=", emqx_http_lib:uri_encode(V)]
            | Acc
        ]
    ).

serialize_body(<<"application/json">>, Body) ->
    jsx:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    query_string(Body).

client_vars(Client, PubSub, Topic) ->
    Client#{
        action => PubSub,
        topic => Topic
    }.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
