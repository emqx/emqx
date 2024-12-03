%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_http_utils).

-export([
    convert_headers/1,
    convert_headers_no_content_type/1,
    default_headers/0,
    default_headers_no_content_type/0,
    transform_header_name/1,
    parse_url/1,
    generate_request/2
]).

-type headers() :: #{binary() => binary()}.
-type request_path() :: binary().
-type request_query() :: binary().

-define(DEFAULT_HTTP_REQUEST_CONTENT_TYPE, <<"application/json">>).

-spec convert_headers(headers()) -> headers().
convert_headers(Headers) ->
    transform_header_name(Headers).

-spec convert_headers_no_content_type(headers()) -> headers().
convert_headers_no_content_type(Headers) ->
    maps:without(
        [<<"content-type">>],
        transform_header_name(Headers)
    ).

-spec default_headers() -> headers().
default_headers() ->
    maps:put(
        <<"content-type">>,
        <<"application/json">>,
        default_headers_no_content_type()
    ).

-spec default_headers_no_content_type() -> headers().
default_headers_no_content_type() ->
    #{
        <<"accept">> => <<"application/json">>,
        <<"cache-control">> => <<"no-cache">>,
        <<"connection">> => <<"keep-alive">>,
        <<"keep-alive">> => <<"timeout=30, max=1000">>
    }.

-spec transform_header_name(headers()) -> headers().
transform_header_name(Headers) ->
    maps:fold(
        fun(K0, V, Acc) ->
            K = list_to_binary(string:to_lower(to_list(K0))),
            maps:put(K, V, Acc)
        end,
        #{},
        Headers
    ).

-spec parse_url(binary()) ->
    {emqx_utils_uri:request_base(), request_path(), request_query()}.
parse_url(Url) ->
    Parsed = emqx_utils_uri:parse(Url),
    case Parsed of
        #{scheme := undefined} ->
            throw({invalid_url, {no_scheme, Url}});
        #{authority := undefined} ->
            throw({invalid_url, {no_host, Url}});
        #{authority := #{userinfo := Userinfo}} when Userinfo =/= undefined ->
            throw({invalid_url, {userinfo_not_supported, Url}});
        #{fragment := Fragment} when Fragment =/= undefined ->
            throw({invalid_url, {fragments_not_supported, Url}});
        _ ->
            case emqx_utils_uri:request_base(Parsed) of
                {ok, Base} ->
                    {Base, emqx_utils_uri:path(Parsed),
                        emqx_maybe:define(emqx_utils_uri:query(Parsed), <<>>)};
                {error, Reason} ->
                    throw({invalid_url, {invalid_base, Reason, Url}})
            end
    end.

generate_request(
    #{
        method := Method,
        headers := Headers,
        base_path_template := BasePathTemplate,
        base_query_template := BaseQueryTemplate,
        body_template := BodyTemplate
    },
    Values
) ->
    Path = emqx_auth_template:render_urlencoded_str(BasePathTemplate, Values),
    Query = emqx_auth_template:render_deep_for_url(BaseQueryTemplate, Values),
    case Method of
        get ->
            Body = emqx_auth_template:render_deep_for_url(BodyTemplate, Values),
            NPath = append_query(Path, Query, Body),
            {ok, {NPath, Headers}};
        _ ->
            try
                ContentType = post_request_content_type(Headers),
                Body = serialize_body(ContentType, BodyTemplate, Values),
                NPathQuery = append_query(Path, Query),
                {ok, {NPathQuery, Headers, Body}}
            catch
                error:{encode_error, _} = Reason ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

post_request_content_type(Headers) ->
    proplists:get_value(<<"content-type">>, Headers, ?DEFAULT_HTTP_REQUEST_CONTENT_TYPE).

append_query(Path, []) ->
    Path;
append_query(Path, Query) ->
    [Path, $?, uri_string:compose_query(Query)].
append_query(Path, Query, Body) ->
    append_query(Path, Query ++ maps:to_list(Body)).

serialize_body(<<"application/json">>, BodyTemplate, ClientInfo) ->
    Body = emqx_auth_template:render_deep_for_json(BodyTemplate, ClientInfo),
    emqx_utils_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, BodyTemplate, ClientInfo) ->
    Body = emqx_auth_template:render_deep_for_url(BodyTemplate, ClientInfo),
    uri_string:compose_query(maps:to_list(Body));
serialize_body(undefined, _BodyTemplate, _ClientInfo) ->
    throw(missing_content_type_header);
serialize_body(ContentType, _BodyTemplate, _ClientInfo) ->
    throw({unknown_content_type_header_value, ContentType}).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

templates_test_() ->
    [
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com?client=${clientid}">>)
        ),
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"/path">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com/path?client=${clientid}">>)
        ),
        ?_assertEqual(
            {#{port => 80, scheme => http, host => "example.com"}, <<"/path">>, <<>>},
            parse_url(<<"http://example.com/path">>)
        )
    ].

-endif.
