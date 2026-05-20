%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_utils).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Template parsing/rendering
-export([
    parse_deep/2,
    parse_str/2,
    parse_sql/3,
    render_deep_for_json/2,
    render_deep_for_url/2,
    render_deep_for_raw/2,
    render_str/2,
    render_urlencoded_str/2,
    render_sql_params/2,
    render_strict/2
]).

%% URL parsing
-export([parse_url/1]).

%% HTTP request/response helpers
-export([generate_request/2]).

-define(DEFAULT_HTTP_REQUEST_CONTENT_TYPE, <<"application/json">>).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Template parsing/rendering

parse_deep(Template, AllowedVars) ->
    Result = emqx_template:parse_deep(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {deep, Template}).

parse_str(Template, AllowedVars) ->
    Result = emqx_template:parse(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {string, Template}).

parse_sql(Template, ReplaceWith, AllowedVars) ->
    {Statement, Result} = emqx_template_sql:parse_prepstmt(
        Template,
        #{parameters => ReplaceWith, strip_double_quote => true}
    ),
    {Statement, handle_disallowed_placeholders(Result, AllowedVars, {string, Template})}.

handle_disallowed_placeholders(Template, AllowedVars, Source) ->
    case emqx_template:validate(AllowedVars, Template) of
        ok ->
            Template;
        {error, Disallowed} ->
            ?tp(warning, "auth_template_invalid", #{
                template => Source,
                reason => Disallowed,
                allowed => #{placeholders => AllowedVars},
                notice =>
                    "Disallowed placeholders will be rendered as is."
                    " However, consider using `${$}` escaping for literal `$` where"
                    " needed to avoid unexpected results."
            }),
            Result = prerender_disallowed_placeholders(Template, AllowedVars),
            case Source of
                {string, _} ->
                    emqx_template:parse(Result);
                {deep, _} ->
                    emqx_template:parse_deep(Result)
            end
    end.

prerender_disallowed_placeholders(Template, AllowedVars) ->
    {Result, _} = emqx_template:render(Template, #{}, #{
        var_trans => fun(Name, _) ->
            % NOTE
            % Rendering disallowed placeholders in escaped form, which will then
            % parse as a literal string.
            case lists:member(Name, AllowedVars) of
                true -> "${" ++ Name ++ "}";
                false -> "${$}{" ++ Name ++ "}"
            end
        end
    }),
    Result.

render_deep_for_json(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string_for_json/2}
    ),
    Term.

render_deep_for_raw(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string_for_raw/2}
    ),
    Term.

render_deep_for_url(Template, Credential) ->
    render_deep_for_raw(Template, Credential).

render_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string/2}
    ),
    unicode:characters_to_binary(String).

render_urlencoded_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_urlencoded_string/2}
    ),
    unicode:characters_to_binary(String).

render_sql_params(ParamList, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Row, _Errors} = emqx_template:render(
        ParamList,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_sql_value/2}
    ),
    Row.

to_urlencoded_string(Name, Value) ->
    case uri_string:compose_query([{<<"q">>, to_string(Name, Value)}]) of
        <<"q=", EncodedBin/binary>> ->
            EncodedBin;
        "q=" ++ EncodedStr ->
            list_to_binary(EncodedStr)
    end.

to_string(Name, Value) ->
    emqx_template:to_string(render_var(Name, Value)).

%% This converter is to generate data structure possibly with non-utf8 strings.
%% It converts to unicode only strings (character lists).

to_string_for_raw(Name, Value) ->
    strings_to_unicode(Name, render_var(Name, Value)).

%% This converter is to generate data structure suitable for JSON serialization.
%% JSON strings are sequences of unicode characters, not bytes.
%% So we force all rendered data to be unicode, not only character lists.

to_string_for_json(Name, Value) ->
    all_to_unicode(Name, render_var(Name, Value)).

strings_to_unicode(_Name, Value) when is_binary(Value) ->
    Value;
strings_to_unicode(Name, Value) when is_list(Value) ->
    to_unicode_binary(Name, Value);
strings_to_unicode(_Name, Value) ->
    emqx_template:to_string(Value).

all_to_unicode(Name, Value) when is_list(Value) orelse is_binary(Value) ->
    to_unicode_binary(Name, Value);
all_to_unicode(_Name, Value) ->
    emqx_template:to_string(Value).

to_unicode_binary(Name, Value) when is_list(Value) orelse is_binary(Value) ->
    try unicode:characters_to_binary(Value) of
        Encoded when is_binary(Encoded) ->
            Encoded;
        _ ->
            error({encode_error, {non_unicode_data, Name}})
    catch
        error:badarg ->
            error({encode_error, {non_unicode_data, Name}})
    end.

to_sql_value(Name, Value) ->
    emqx_utils_sql:to_sql_value(render_var(Name, Value)).

render_var(_, undefined) ->
    % NOTE
    % Any allowed but undefined binding will be replaced with empty string, even when
    % rendering SQL values.
    <<>>;
render_var(?VAR_CERT_PEM, Value) ->
    base64:encode(Value);
render_var(?VAR_PEERHOST, Value) ->
    inet:ntoa(Value);
render_var(?VAR_PASSWORD, Value) ->
    iolist_to_binary(Value);
render_var(_Name, Value) ->
    Value.

render_strict(Topic, ClientInfo) ->
    emqx_template:render_strict(Topic, rename_client_info_vars(ClientInfo)).

rename_client_info_vars(ClientInfo) ->
    Renames = [
        {cn, cert_common_name},
        {dn, cert_subject},
        {protocol, proto_name}
    ],
    lists:foldl(
        fun({Old, New}, Acc) ->
            emqx_utils_maps:rename(Old, New, Acc)
        end,
        ClientInfo,
        Renames
    ).

%%--------------------------------------------------------------------
%% URL parsing

-spec parse_url(binary()) ->
    {_Base :: emqx_utils_uri:request_base(), _Path :: binary(), _Query :: binary()}.
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

%%--------------------------------------------------------------------
%% HTTP request/response helpers

generate_request(
    #{
        method := Method,
        headers := Headers0,
        base_path_template := BasePathTemplate,
        base_query_template := BaseQueryTemplate,
        body_template := BodyTemplate
    },
    Values
) ->
    Path = render_urlencoded_str(BasePathTemplate, Values),
    Query = render_deep_for_url(BaseQueryTemplate, Values),
    Headers = emqx_auth_utils:render_deep_for_raw(Headers0, Values),
    case validate_headers(Headers) of
        ok ->
            case Method of
                get ->
                    Body = render_deep_for_url(BodyTemplate, Values),
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
            end;
        {error, _} = Error ->
            Error
    end.

%% Defense-in-depth: reject HTTP header names/values that contain bytes
%% capable of splitting the request line (NUL, CR, LF). Header templates may
%% interpolate variables that originated from outside the broker (e.g.
%% ${cert_common_name} from a PROXY-Protocol v2 SSL TLV, ${peerhost}, or
%% client-attribute computations). The primary mitigation rejects PP2 cert
%% TLVs with control bytes at ingestion time; this is a second wall.
validate_headers(Headers) when is_list(Headers) ->
    validate_headers_list(Headers);
validate_headers(Headers) when is_map(Headers) ->
    validate_headers_list(maps:to_list(Headers)).

validate_headers_list([]) ->
    ok;
validate_headers_list([{Name, Value} | Rest]) ->
    case header_byte_check(Name) of
        ok ->
            case header_byte_check(Value) of
                ok -> validate_headers_list(Rest);
                {error, Reason} -> {error, {bad_http_header_value, Name, Reason}}
            end;
        {error, Reason} ->
            {error, {bad_http_header_name, Reason}}
    end.

header_byte_check(Bin) when is_binary(Bin) ->
    header_byte_check_bin(Bin);
header_byte_check(_NonBin) ->
    ok.

header_byte_check_bin(<<>>) -> ok;
header_byte_check_bin(<<0, _/binary>>) -> {error, contains_nul};
header_byte_check_bin(<<$\r, _/binary>>) -> {error, contains_cr};
header_byte_check_bin(<<$\n, _/binary>>) -> {error, contains_lf};
header_byte_check_bin(<<_, Rest/binary>>) -> header_byte_check_bin(Rest).

post_request_content_type(Headers) ->
    proplists:get_value(<<"content-type">>, Headers, ?DEFAULT_HTTP_REQUEST_CONTENT_TYPE).

append_query(Path, []) ->
    Path;
append_query(Path, Query) ->
    [Path, $?, uri_string:compose_query(Query)].
append_query(Path, Query, Body) ->
    append_query(Path, Query ++ maps:to_list(Body)).

serialize_body(<<"application/json">>, BodyTemplate, ClientInfo) ->
    Body = emqx_auth_utils:render_deep_for_json(BodyTemplate, ClientInfo),
    emqx_utils_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, BodyTemplate, ClientInfo) ->
    Body = emqx_auth_utils:render_deep_for_url(BodyTemplate, ClientInfo),
    uri_string:compose_query(maps:to_list(Body));
serialize_body(undefined, _BodyTemplate, _ClientInfo) ->
    throw(missing_content_type_header);
serialize_body(ContentType, _BodyTemplate, _ClientInfo) ->
    throw({unknown_content_type_header_value, ContentType}).

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
