%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_http_utils).

-export([
    convert_headers/1,
    convert_headers_no_content_type/1,
    default_headers/0,
    default_headers_no_content_type/0,
    transform_header_name/1,
    parse_url/1,
    parse_url_template/1,
    is_templated_host_url/1,
    parse_allowed_hosts/1,
    validate_allowed_hosts_field/1,
    render_host/3,
    one_off_request/5,
    do_one_off_request/5,
    generate_request/2
]).

-type headers() :: #{binary() => binary()}.
-type request_path() :: binary().
-type request_query() :: binary().
-type allowed_host() :: {exact, binary()} | {suffix, binary()}.
-type one_off_base() :: #{
    scheme := http | https,
    host_template := emqx_template:t(),
    port := inet:port_number(),
    allowed_hosts := [allowed_host()],
    connect_timeout := timeout(),
    ssl_opts := [ssl:tls_client_option()]
}.

-export_type([allowed_host/0, one_off_base/0]).

-define(DEFAULT_HTTP_REQUEST_CONTENT_TYPE, <<"application/json">>).

%% Rendered host must be a plain DNS name or IPv4 address:
%% non-empty labels of [a-z0-9_-] separated by dots.
-define(HOSTNAME_RE, "^[a-z0-9_-]+(\\.[a-z0-9_-]+)*$").
-define(MAX_HOSTNAME_LEN, 253).

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

-doc """
Parse a URL with a static (non-templated) authority.
Throws `{invalid_url, _}` if the URL is invalid or its host contains template placeholders.
""".
-spec parse_url(binary()) ->
    {emqx_utils_uri:request_base(), request_path(), request_query()}.
parse_url(Url) ->
    case parse_url_template(Url) of
        {static, Parsed} ->
            Parsed;
        {dynamic, _} ->
            throw({invalid_url, {templated_host_not_supported, Url}})
    end.

-doc """
Parse a URL whose host part may contain template placeholders (`${...}`).

Returns `{static, {RequestBase, Path, Query}}` when the authority is fixed, or
`{dynamic, #{scheme, host_template, port, path, query}}` when the host is templated.
Only the host may be templated: the scheme must be literal `http`/`https` and
the port, if present, must be a literal integer.
""".
-spec parse_url_template(binary()) ->
    {static, {emqx_utils_uri:request_base(), request_path(), request_query()}}
    | {dynamic, #{
        scheme := http | https,
        host_template := binary(),
        port := inet:port_number(),
        path := request_path(),
        query := request_query()
    }}.
parse_url_template(Url) ->
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
        #{authority := #{host := Host}} ->
            case is_templated(Host) of
                false ->
                    {static, static_url(Parsed, Url)};
                true ->
                    {dynamic, dynamic_url(Parsed, Url)}
            end
    end.

-doc """
Check whether the host part of the URL contains template placeholders.
Returns `false` also for URLs that fail to parse.
""".
-spec is_templated_host_url(binary()) -> boolean().
is_templated_host_url(Url) ->
    case emqx_utils_uri:parse(Url) of
        #{authority := #{host := Host}} ->
            is_templated(Host);
        _ ->
            false
    end.

static_url(Parsed, Url) ->
    case emqx_utils_uri:request_base(Parsed) of
        {ok, Base} ->
            {Base, emqx_utils_uri:path(Parsed),
                emqx_maybe:define(emqx_utils_uri:query(Parsed), <<>>)};
        {error, Reason} ->
            throw({invalid_url, {invalid_base, Reason, Url}})
    end.

dynamic_url(
    #{scheme := SchemeBin, authority := #{host := Host, host_type := HostType, port := Port}} =
        Parsed,
    Url
) ->
    HostType =:= regular orelse throw({invalid_url, {unsupported_templated_host, Url}}),
    Scheme = parse_scheme(SchemeBin, Url),
    #{
        scheme => Scheme,
        host_template => Host,
        port => emqx_maybe:define(Port, default_port(Scheme)),
        path => emqx_utils_uri:path(Parsed),
        query => emqx_maybe:define(emqx_utils_uri:query(Parsed), <<>>)
    }.

parse_scheme(<<"http">>, _Url) -> http;
parse_scheme(<<"https">>, _Url) -> https;
parse_scheme(_, Url) -> throw({invalid_url, {unsupported_scheme, Url}}).

default_port(http) -> 80;
default_port(https) -> 443.

is_templated(Bin) ->
    binary:match(Bin, <<"${">>) =/= nomatch.

-doc """
Compile the `allowed_hosts` config list.
An entry is either an exact hostname (`auth.example.com`) or a wildcard
pattern (`*.auth.example.com`) matching any host under that suffix.
Throws `{invalid_allowed_host, Entry}` on malformed entries.
""".
-spec parse_allowed_hosts([binary()]) -> [allowed_host()].
parse_allowed_hosts(Hosts) when is_list(Hosts) ->
    lists:map(fun parse_allowed_host/1, Hosts);
parse_allowed_hosts(Other) ->
    throw({invalid_allowed_host, Other}).

parse_allowed_host(<<"*.", Suffix0/binary>> = Entry) ->
    Suffix = string:lowercase(Suffix0),
    is_valid_hostname(Suffix) orelse throw({invalid_allowed_host, Entry}),
    {suffix, <<".", Suffix/binary>>};
parse_allowed_host(Host0) when is_binary(Host0) ->
    Host = string:lowercase(Host0),
    is_valid_hostname(Host) orelse throw({invalid_allowed_host, Host0}),
    {exact, Host};
parse_allowed_host(Other) ->
    throw({invalid_allowed_host, Other}).

-doc "Schema validator for the `allowed_hosts` field.".
validate_allowed_hosts_field(Hosts) ->
    try
        _ = parse_allowed_hosts(Hosts),
        ok
    catch
        throw:{invalid_allowed_host, Entry} ->
            {error, {invalid_allowed_host, Entry}}
    end.

-doc """
Render a host template against the client credential/request values and check
the result against the compiled allowlist. Fails closed: missing template
bindings, malformed rendered hostnames and hosts not covered by the allowlist
all yield an error.
""".
-spec render_host(emqx_template:t(), [allowed_host()], map()) ->
    {ok, binary()} | {error, term()}.
render_host(HostTemplate, AllowedHosts, Values) ->
    try emqx_auth_template:render_strict(HostTemplate, Values) of
        Rendered ->
            Host = string:lowercase(iolist_to_binary(Rendered)),
            case is_valid_hostname(Host) of
                true ->
                    case is_host_allowed(Host, AllowedHosts) of
                        true -> {ok, Host};
                        false -> {error, {host_not_allowed, Host}}
                    end;
                false ->
                    {error, {rendered_host_invalid, Host}}
            end
    catch
        _:Reason ->
            {error, {failed_to_render_host, Reason}}
    end.

is_valid_hostname(Host) when
    is_binary(Host), byte_size(Host) > 0, byte_size(Host) =< ?MAX_HOSTNAME_LEN
->
    re:run(Host, ?HOSTNAME_RE, [{capture, none}]) =:= match;
is_valid_hostname(_) ->
    false.

is_host_allowed(Host, AllowedHosts) ->
    lists:any(
        fun
            ({exact, Allowed}) ->
                Host =:= Allowed;
            ({suffix, Suffix}) ->
                SLen = byte_size(Suffix),
                HLen = byte_size(Host),
                HLen > SLen andalso binary:part(Host, HLen - SLen, SLen) =:= Suffix
        end,
        AllowedHosts
    ).

-doc """
Issue a one-off HTTP request to a per-request rendered host, bypassing any
connection pool. The request tuple is the one produced by `generate_request/2`.
The result shape is the same as the pooled (`ehttpc`) query path:
`{ok, Status, Headers} | {ok, Status, Headers, Body} | {error, Reason}`.
""".
-spec one_off_request(one_off_base(), atom(), timeout(), map(), tuple()) ->
    {ok, pos_integer(), list()} | {ok, pos_integer(), list(), binary()} | {error, term()}.
one_off_request(
    #{
        scheme := Scheme,
        host_template := HostTemplate,
        port := Port,
        allowed_hosts := AllowedHosts,
        connect_timeout := ConnectTimeout,
        ssl_opts := SslOpts
    },
    Method,
    Timeout,
    Values,
    Request
) ->
    maybe
        {ok, Host} ?= render_host(HostTemplate, AllowedHosts, Values),
        Url = format_url(Scheme, Host, Port, request_path_query(Request)),
        ReqOpts = [
            {connect_timeout, ConnectTimeout},
            {recv_timeout, Timeout},
            {pool, false}
            | scheme_transport_opts(Scheme, SslOpts)
        ],
        ?MODULE:do_one_off_request(
            Method, Url, request_headers(Request), request_body(Request), ReqOpts
        )
    end.

-doc "Exported for mocking in tests; do not call directly.".
do_one_off_request(Method, Url, Headers, Body, ReqOpts) ->
    case hackney:request(Method, Url, Headers, Body, ReqOpts) of
        {ok, StatusCode, RespHeaders, ClientRef} ->
            case hackney:body(ClientRef) of
                {ok, _} when StatusCode =:= 204 ->
                    {ok, StatusCode, RespHeaders};
                {ok, RespBody} ->
                    {ok, StatusCode, RespHeaders, RespBody};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

request_path_query({PathQuery, _Headers}) -> PathQuery;
request_path_query({PathQuery, _Headers, _Body}) -> PathQuery.

request_headers({_PathQuery, Headers}) -> Headers;
request_headers({_PathQuery, Headers, _Body}) -> Headers.

request_body({_PathQuery, _Headers}) -> <<>>;
request_body({_PathQuery, _Headers, Body}) -> Body.

format_url(Scheme, Host, Port, PathQuery0) ->
    PathQuery = iolist_to_binary(PathQuery0),
    Sep =
        case PathQuery of
            <<"/", _/binary>> -> <<>>;
            _ -> <<"/">>
        end,
    iolist_to_binary([
        atom_to_binary(Scheme), <<"://">>, Host, $:, integer_to_binary(Port), Sep, PathQuery
    ]).

scheme_transport_opts(https, SslOpts) -> [{ssl_options, SslOpts}];
scheme_transport_opts(http, _SslOpts) -> [].

generate_request(
    #{
        method := Method,
        headers_template := HeadersTemplate,
        base_path_template := BasePathTemplate,
        base_query_template := BaseQueryTemplate,
        body_template := BodyTemplate
    },
    Values
) ->
    Path = emqx_auth_template:render_urlencoded_str(BasePathTemplate, Values),
    Query = emqx_auth_template:render_deep_for_url(BaseQueryTemplate, Values),
    Headers = emqx_auth_template:render_deep_for_raw(HeadersTemplate, Values),
    case validate_headers(Headers) of
        ok ->
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
    case emqx_utils:http_header_byte_check(Name) of
        ok ->
            case emqx_utils:http_header_byte_check(Value) of
                ok -> validate_headers_list(Rest);
                {error, Reason} -> {error, {bad_http_header_value, Name, Reason}}
            end;
        {error, Reason} ->
            {error, {bad_http_header_name, Reason}}
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

parse_url_template_test_() ->
    [
        ?_assertEqual(
            {static, {#{port => 80, scheme => http, host => "example.com"}, <<"/path">>, <<>>}},
            parse_url_template(<<"http://example.com/path">>)
        ),
        ?_assertEqual(
            {dynamic, #{
                scheme => https,
                host_template => <<"${client_attrs.tns}.auth.example.com">>,
                port => 443,
                path => <<"/authn">>,
                query => <<>>
            }},
            parse_url_template(<<"https://${client_attrs.tns}.auth.example.com/authn">>)
        ),
        ?_assertEqual(
            {dynamic, #{
                scheme => http,
                host_template => <<"${username}.example.com">>,
                port => 8080,
                path => <<"">>,
                query => <<"client=${clientid}">>
            }},
            parse_url_template(<<"http://${username}.example.com:8080?client=${clientid}">>)
        ),
        ?_assertThrow(
            {invalid_url, {templated_host_not_supported, _}},
            parse_url(<<"http://${username}.example.com/path">>)
        ),
        ?_assertThrow(
            {invalid_url, {unsupported_scheme, _}},
            parse_url_template(<<"ftp://${username}.example.com/path">>)
        ),
        %% templated port makes the host "loose" (contains a colon)
        ?_assertThrow(
            {invalid_url, {unsupported_templated_host, _}},
            parse_url_template(<<"http://${username}.example.com:${port}/path">>)
        ),
        ?_assertEqual(true, is_templated_host_url(<<"http://${username}.example.com">>)),
        ?_assertEqual(false, is_templated_host_url(<<"http://example.com/${username}">>)),
        ?_assertEqual(false, is_templated_host_url(<<"not a url">>))
    ].

allowed_hosts_test_() ->
    [
        ?_assertEqual(
            [{exact, <<"auth.example.com">>}, {suffix, <<".auth.example.com">>}],
            parse_allowed_hosts([<<"Auth.Example.Com">>, <<"*.auth.example.com">>])
        ),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<>>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"*.">>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"bad host">>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"host:8080">>])),
        ?_assertEqual(ok, validate_allowed_hosts_field([<<"a.example.com">>])),
        ?_assertMatch({error, _}, validate_allowed_hosts_field([<<"a/b">>]))
    ].

render_host_test_() ->
    Template = fun(Str) ->
        {_Vars, T} = emqx_auth_template:parse_str(Str, ["username", {var_namespace, "client_attrs"}]),
        T
    end,
    AllowedHosts = [{suffix, <<".auth.example.com">>}, {exact, <<"fallback.example.com">>}],
    [
        ?_assertEqual(
            {ok, <<"t1.auth.example.com">>},
            render_host(
                Template(<<"${client_attrs.tns}.auth.example.com">>),
                AllowedHosts,
                #{client_attrs => #{<<"tns">> => <<"t1">>}}
            )
        ),
        ?_assertEqual(
            {ok, <<"fallback.example.com">>},
            render_host(
                Template(<<"fallback.EXAMPLE.com">>),
                AllowedHosts,
                #{}
            )
        ),
        ?_assertMatch(
            {error, {host_not_allowed, <<"t1.evil.example.com">>}},
            render_host(
                Template(<<"${client_attrs.tns}.evil.example.com">>),
                AllowedHosts,
                #{client_attrs => #{<<"tns">> => <<"t1">>}}
            )
        ),
        %% missing binding: fail-closed
        ?_assertMatch(
            {error, {failed_to_render_host, _}},
            render_host(
                Template(<<"${client_attrs.tns}.auth.example.com">>),
                AllowedHosts,
                #{}
            )
        ),
        %% rendered host with URL metacharacters: fail-closed
        ?_assertMatch(
            {error, {rendered_host_invalid, _}},
            render_host(
                Template(<<"${username}.auth.example.com">>),
                AllowedHosts,
                #{username => <<"evil.com/#">>}
            )
        ),
        %% exact match must not be bypassed by a subdomain
        ?_assertMatch(
            {error, {host_not_allowed, _}},
            render_host(
                Template(<<"${username}.fallback.example.com">>),
                [{exact, <<"fallback.example.com">>}],
                #{username => <<"sub">>}
            )
        )
    ].

format_url_test_() ->
    [
        ?_assertEqual(
            <<"https://h.example.com:443/a/b?q=1">>,
            format_url(https, <<"h.example.com">>, 443, [<<"/a/b">>, $?, <<"q=1">>])
        ),
        ?_assertEqual(
            <<"http://h.example.com:80/">>,
            format_url(http, <<"h.example.com">>, 80, <<>>)
        ),
        ?_assertEqual(
            <<"http://h.example.com:80/?q=1">>,
            format_url(http, <<"h.example.com">>, 80, <<"?q=1">>)
        )
    ].

-endif.
