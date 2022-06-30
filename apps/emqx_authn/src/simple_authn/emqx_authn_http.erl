%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_http).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    validations/0
]).

-export([
    headers_no_content_type/1,
    headers/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-http".

roots() ->
    [
        {?CONF_NS,
            hoconsc:mk(
                hoconsc:union(refs()),
                #{}
            )}
    ].

fields(get) ->
    [
        {method, #{type => get, required => true, default => get, desc => ?DESC(method)}},
        {headers, fun headers_no_content_type/1}
    ] ++ common_fields();
fields(post) ->
    [
        {method, #{type => post, required => true, default => post, desc => ?DESC(method)}},
        {headers, fun headers/1}
    ] ++ common_fields().

desc(get) ->
    ?DESC(get);
desc(post) ->
    ?DESC(post);
desc(_) ->
    undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(http)},
        {url, fun url/1},
        {body,
            hoconsc:mk(map([{fuzzy, term(), binary()}]), #{
                required => false, desc => ?DESC(body)
            })},
        {request_timeout, fun request_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++
        maps:to_list(
            maps:without(
                [
                    base_url,
                    pool_type
                ],
                maps:from_list(emqx_connector_http:fields(config))
            )
        ).

validations() ->
    [
        {check_ssl_opts, fun check_ssl_opts/1},
        {check_headers, fun check_headers/1}
    ].

url(type) -> binary();
url(desc) -> ?DESC(?FUNCTION_NAME);
url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
url(required) -> true;
url(_) -> undefined.

headers(type) ->
    map();
headers(desc) ->
    ?DESC(?FUNCTION_NAME);
headers(converter) ->
    fun(Headers) ->
        maps:merge(default_headers(), transform_header_name(Headers))
    end;
headers(default) ->
    default_headers();
headers(_) ->
    undefined.

headers_no_content_type(type) ->
    map();
headers_no_content_type(desc) ->
    ?DESC(?FUNCTION_NAME);
headers_no_content_type(converter) ->
    fun(Headers) ->
        maps:without(
            [<<"content-type">>],
            maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
        )
    end;
headers_no_content_type(default) ->
    default_headers_no_content_type();
headers_no_content_type(_) ->
    undefined.

request_timeout(type) -> emqx_schema:duration_ms();
request_timeout(desc) -> ?DESC(?FUNCTION_NAME);
request_timeout(default) -> <<"5s">>;
request_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [
        hoconsc:ref(?MODULE, get),
        hoconsc:ref(?MODULE, post)
    ].

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {Config, State} = parse_config(Config0),
    {ok, _Data} = emqx_authn_utils:create_resource(
        ResourceId,
        emqx_connector_http,
        Config
    ),
    {ok, State#{resource_id => ResourceId}}.

update(Config0, #{resource_id := ResourceId} = _State) ->
    {Config, NState} = parse_config(Config0),
    case emqx_authn_utils:update_resource(emqx_connector_http, Config, ResourceId) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    Credential,
    #{
        resource_id := ResourceId,
        method := Method,
        request_timeout := RequestTimeout
    } = State
) ->
    Request = generate_request(Credential, State),
    case emqx_resource:query(ResourceId, {Method, Request, RequestTimeout}) of
        {ok, 204, _Headers} ->
            {ok, #{is_superuser => false}};
        {ok, 200, Headers, Body} ->
            handle_response(Headers, Body);
        {ok, _StatusCode, _Headers} = Response ->
            log_response(ResourceId, Response),
            ignore;
        {ok, _StatusCode, _Headers, _Body} = Response ->
            log_response(ResourceId, Response),
            ignore;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_server_query_failed",
                resource => ResourceId,
                reason => Reason
            }),
            ignore
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

default_headers() ->
    maps:put(
        <<"content-type">>,
        <<"application/json">>,
        default_headers_no_content_type()
    ).

default_headers_no_content_type() ->
    #{
        <<"accept">> => <<"application/json">>,
        <<"cache-control">> => <<"no-cache">>,
        <<"connection">> => <<"keep-alive">>,
        <<"keep-alive">> => <<"timeout=30, max=1000">>
    }.

transform_header_name(Headers) ->
    maps:fold(
        fun(K0, V, Acc) ->
            K = list_to_binary(string:to_lower(to_list(K0))),
            maps:put(K, V, Acc)
        end,
        #{},
        Headers
    ).

check_ssl_opts(Conf) ->
    {BaseUrl, _Path, _Query} = parse_url(get_conf_val("url", Conf)),
    case BaseUrl of
        <<"https://", _/binary>> ->
            case get_conf_val("ssl.enable", Conf) of
                true -> ok;
                false -> false
            end;
        <<"http://", _/binary>> ->
            ok
    end.

check_headers(Conf) ->
    Method = to_bin(get_conf_val("method", Conf)),
    Headers = get_conf_val("headers", Conf),
    Method =:= <<"post">> orelse (not maps:is_key(<<"content-type">>, Headers)).

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

parse_config(
    #{
        method := Method,
        url := RawUrl,
        headers := Headers,
        request_timeout := RequestTimeout
    } = Config
) ->
    {BaseUrl0, Path, Query} = parse_url(RawUrl),
    {ok, BaseUrl} = emqx_http_lib:uri_parse(BaseUrl0),
    State = #{
        method => Method,
        path => Path,
        headers => ensure_header_name_type(Headers),
        base_path_templete => emqx_authn_utils:parse_str(Path),
        base_query_template => emqx_authn_utils:parse_deep(
            cow_qs:parse_qs(to_bin(Query))
        ),
        body_template => emqx_authn_utils:parse_deep(maps:get(body, Config, #{})),
        request_timeout => RequestTimeout
    },
    {Config#{base_url => BaseUrl, pool_type => random}, State}.

generate_request(Credential, #{
    method := Method,
    headers := Headers0,
    base_path_templete := BasePathTemplate,
    base_query_template := BaseQueryTemplate,
    body_template := BodyTemplate
}) ->
    Headers = maps:to_list(Headers0),
    Path = emqx_authn_utils:render_str(BasePathTemplate, Credential),
    Query = emqx_authn_utils:render_deep(BaseQueryTemplate, Credential),
    Body = emqx_authn_utils:render_deep(BodyTemplate, Credential),
    case Method of
        get ->
            NPathQuery = append_query(to_list(Path), to_list(Query) ++ maps:to_list(Body)),
            {NPathQuery, Headers};
        post ->
            NPathQuery = append_query(to_list(Path), to_list(Query)),
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            NBody = serialize_body(ContentType, Body),
            {NPathQuery, Headers, NBody}
    end.

append_query(Path, []) ->
    encode_path(Path);
append_query(Path, Query) ->
    encode_path(Path) ++ "?" ++ binary_to_list(qs(Query)).

qs(KVs) ->
    qs(KVs, []).

qs([], Acc) ->
    <<$&, Qs/binary>> = iolist_to_binary(lists:reverse(Acc)),
    Qs;
qs([{K, V} | More], Acc) ->
    qs(More, [["&", uri_encode(K), "=", uri_encode(V)] | Acc]).

serialize_body(<<"application/json">>, Body) ->
    emqx_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    qs(maps:to_list(Body)).

handle_response(Headers, Body) ->
    ContentType = proplists:get_value(<<"content-type">>, Headers),
    case safely_parse_body(ContentType, Body) of
        {ok, NBody} ->
            case maps:get(<<"result">>, NBody, <<"ignore">>) of
                <<"allow">> ->
                    Res = emqx_authn_utils:is_superuser(NBody),
                    %% TODO: Return by user property
                    {ok, Res#{user_property => maps:get(<<"user_property">>, NBody, #{})}};
                <<"deny">> ->
                    {error, not_authorized};
                <<"ignore">> ->
                    ignore;
                _ ->
                    ignore
            end;
        {error, _Reason} ->
            ignore
    end.

safely_parse_body(ContentType, Body) ->
    try
        parse_body(ContentType, Body)
    catch
        _Class:_Reason ->
            {error, invalid_body}
    end.

parse_body(<<"application/json", _/binary>>, Body) ->
    {ok, emqx_json:decode(Body, [return_maps])};
parse_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    Flags = [<<"result">>, <<"is_superuser">>],
    RawMap = maps:from_list(cow_qs:parse_qs(Body)),
    NBody = maps:with(Flags, RawMap),
    {ok, NBody};
parse_body(ContentType, _) ->
    {error, {unsupported_content_type, ContentType}}.

may_append_body(Output, {ok, _, _, Body}) ->
    Output#{body => Body};
may_append_body(Output, {ok, _, _}) ->
    Output.

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

encode_path(Path) ->
    Parts = string:split(Path, "/", all),
    lists:flatten(["/" ++ Part || Part <- lists:map(fun uri_encode/1, Parts)]).

log_response(ResourceId, Other) ->
    Output = may_append_body(#{resource => ResourceId}, Other),
    case erlang:element(2, Other) of
        Code5xx when Code5xx >= 500 andalso Code5xx < 600 ->
            ?SLOG(error, Output#{
                msg => "http_server_error",
                code => Code5xx
            });
        Code4xx when Code4xx >= 400 andalso Code4xx < 500 ->
            ?SLOG(warning, Output#{
                msg => "refused_by_http_server",
                code => Code4xx
            });
        OtherCode ->
            ?SLOG(error, Output#{
                msg => "undesired_response_code",
                code => OtherCode
            })
    end.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(B) when is_binary(B) ->
    B;
to_bin(L) when is_list(L) ->
    list_to_binary(L).

get_conf_val(Name, Conf) ->
    hocon_maps:get(?CONF_NS ++ "." ++ Name, Conf).

ensure_header_name_type(Headers) ->
    Fun = fun
        (Key, _Val, Acc) when is_binary(Key) ->
            Acc;
        (Key, Val, Acc) when is_atom(Key) ->
            Acc2 = maps:remove(Key, Acc),
            BinKey = erlang:atom_to_binary(Key),
            Acc2#{BinKey => Val}
    end,
    maps:fold(Fun, Headers, Headers).
