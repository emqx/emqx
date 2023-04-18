%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4,
    merge_defaults/1,
    parse_url/1
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(PLACEHOLDERS, [
    <<?VAR_USERNAME>>,
    <<?VAR_CLIENTID>>,
    <<?VAR_PEERHOST>>,
    <<?VAR_PROTONAME>>,
    <<?VAR_MOUNTPOINT>>,
    <<?VAR_TOPIC>>,
    <<?VAR_ACTION>>,
    <<?VAR_CERT_SUBJECT>>,
    <<?VAR_CERT_CN_NAME>>
]).

-define(PLACEHOLDERS_FOR_RICH_ACTIONS, [
    <<?VAR_QOS>>,
    <<?VAR_RETAIN>>
]).

description() ->
    "AuthZ with http".

create(Config) ->
    NConfig = parse_config(Config),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_bridge_http_connector, NConfig),
    NConfig#{annotations => #{id => ResourceId}}.

update(Config) ->
    NConfig = parse_config(Config),
    case emqx_authz_utils:update_resource(emqx_bridge_http_connector, NConfig) of
        {error, Reason} -> error({load_config_error, Reason});
        {ok, Id} -> NConfig#{annotations => #{id => Id}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        type := http,
        annotations := #{id := ResourceID},
        method := Method,
        request_timeout := RequestTimeout
    } = Config
) ->
    Request = generate_request(Action, Topic, Client, Config),
    case emqx_resource:simple_sync_query(ResourceID, {Method, Request, RequestTimeout}) of
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
            ?tp(authz_http_request_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "http_server_query_failed",
                resource => ResourceID,
                reason => Reason
            }),
            ignore
    end.

merge_defaults(#{<<"headers">> := Headers} = Source) ->
    NewHeaders =
        case Source of
            #{<<"method">> := <<"get">>} ->
                (emqx_authz_http_schema:headers_no_content_type(converter))(Headers);
            #{<<"method">> := <<"post">>} ->
                (emqx_authz_http_schema:headers(converter))(Headers);
            _ ->
                Headers
        end,
    Source#{<<"headers">> => NewHeaders};
merge_defaults(Source) ->
    Source.

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
        base_path_templete => emqx_authz_utils:parse_str(Path, placeholders()),
        base_query_template => emqx_authz_utils:parse_deep(
            cow_qs:parse_qs(to_bin(Query)),
            placeholders()
        ),
        body_template => emqx_authz_utils:parse_deep(
            maps:to_list(maps:get(body, Conf, #{})),
            placeholders()
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
                            {BaseUrl, <<"/", Path/binary>>, QueryString};
                        [Path] ->
                            {BaseUrl, <<"/", Path/binary>>, <<>>}
                    end;
                [HostPort] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), <<>>, <<>>}
            end;
        [Url] ->
            throw({invalid_url, Url})
    end.

generate_request(
    Action,
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
    Values = client_vars(Client, Action, Topic),
    Path = emqx_authz_utils:render_urlencoded_str(BasePathTemplate, Values),
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
    to_list(Path);
append_query(Path, Query) ->
    to_list(Path) ++ "?" ++ to_list(query_string(Query)).

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

serialize_body(<<"application/json">>, Body) ->
    emqx_utils_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    query_string(Body).

client_vars(Client, Action, Topic) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    Vars#{topic => Topic}.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(X) -> X.

placeholders() ->
    placeholders(emqx_authz:feature_available(rich_actions)).

placeholders(true) ->
    ?PLACEHOLDERS ++ ?PLACEHOLDERS_FOR_RICH_ACTIONS;
placeholders(false) ->
    ?PLACEHOLDERS.
