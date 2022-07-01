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
    create/1,
    update/1,
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

create(Config) ->
    NConfig = parse_config(Config),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_connector_http, NConfig),
    NConfig#{annotations => #{id => ResourceId}}.

update(Config) ->
    NConfig = parse_config(Config),
    case emqx_authz_utils:update_resource(emqx_connector_http, NConfig) of
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
        {ok, 204, _Headers} ->
            {matched, allow};
        {ok, 200, Headers, Body} ->
            ContentType = emqx_authz_utils:content_type(Headers),
            case emqx_authz_utils:parse_http_resp_body(ContentType, Body) of
                error ->
                    ?SLOG(error, #{
                        msg => authz_http_response_incorrect,
                        content_type => ContentType,
                        body => Body
                    }),
                    nomatch;
                Result ->
                    {matched, Result}
            end;
        {ok, Status, Headers} ->
            log_nomtach_msg(Status, Headers, undefined),
            nomatch;
        {ok, Status, Headers, Body} ->
            log_nomtach_msg(Status, Headers, Body),
            nomatch;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_server_query_failed",
                resource => ResourceID,
                reason => Reason
            }),
            ignore
    end.

log_nomtach_msg(Status, Headers, Body) ->
    ?SLOG(
        debug,
        #{
            msg => unexpected_authz_http_response,
            status => Status,
            content_type => emqx_authz_utils:content_type(Headers),
            body => Body
        }
    ).

parse_config(
    #{
        url := RawUrl,
        method := Method,
        headers := Headers,
        request_timeout := ReqTimeout
    } = Conf
) ->
    {BaseUrl0, Path, Query} = parse_url(RawUrl),
    {ok, BaseUrl} = emqx_http_lib:uri_parse(BaseUrl0),
    Conf#{
        method => Method,
        base_url => BaseUrl,
        headers => Headers,
        base_path_templete => emqx_authz_utils:parse_str(Path, ?PLACEHOLDERS),
        base_query_template => emqx_authz_utils:parse_deep(
            cow_qs:parse_qs(to_bin(Query)),
            ?PLACEHOLDERS
        ),
        body_template => emqx_authz_utils:parse_deep(
            maps:to_list(maps:get(body, Conf, #{})),
            ?PLACEHOLDERS
        ),
        request_timeout => ReqTimeout,
        %% pool_type default value `random`
        pool_type => random
    }.

parse_url(Url) ->
    case string:split(Url, "//", leading) of
        [Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [HostPort, Remaining] ->
                    BaseUrl = iolist_to_binary([Scheme, "//", HostPort]),
                    case string:split(Remaining, "?", leading) of
                        [Path, QueryString] ->
                            {BaseUrl, Path, QueryString};
                        [Path] ->
                            {BaseUrl, Path, <<>>}
                    end;
                [HostPort] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), <<>>, <<>>}
            end;
        [Url] ->
            throw({invalid_url, Url})
    end.

generate_request(
    PubSub,
    Topic,
    Client,
    #{
        method := Method,
        headers := Headers,
        base_path_templete := BasePathTemplate,
        base_query_template := BaseQueryTemplate,
        body_template := BodyTemplate
    }
) ->
    Values = client_vars(Client, PubSub, Topic),
    Path = emqx_authz_utils:render_str(BasePathTemplate, Values),
    Query = emqx_authz_utils:render_deep(BaseQueryTemplate, Values),
    Body = emqx_authz_utils:render_deep(BodyTemplate, Values),
    case Method of
        get ->
            NPath = append_query(Path, Query ++ Body),
            {NPath, Headers};
        _ ->
            NPath = append_query(Path, Query),
            NBody = serialize_body(
                proplists:get_value(<<"accept">>, Headers, <<"application/json">>),
                Body
            ),
            {NPath, Headers, NBody}
    end.

append_query(Path, []) ->
    encode_path(Path);
append_query(Path, Query) ->
    encode_path(Path) ++ "?" ++ to_list(query_string(Query)).

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
    query_string(More, [["&", uri_encode(K), "=", uri_encode(V)] | Acc]).

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

encode_path(Path) ->
    Parts = string:split(Path, "/", all),
    lists:flatten(["/" ++ Part || Part <- lists:map(fun uri_encode/1, Parts)]).

serialize_body(<<"application/json">>, Body) ->
    jsx:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    query_string(Body).

client_vars(Client, PubSub, Topic) ->
    Client#{
        action => PubSub,
        topic => Topic
    }.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(X) -> X.
