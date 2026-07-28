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
    parse_url_template/2,
    request_base/1,
    is_templated_host_url/1,
    ensure_pool/2,
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
-type hostname_resolution() :: static | dynamic.
%% Parsed URL; the host is a literal or, with dynamic resolution, may be a template.
-type url_template() :: #{
    scheme := http | https,
    host := {static, binary()} | {template, binary()},
    port := inet:port_number(),
    path := request_path(),
    query := request_query()
}.
%% Base for dynamic-resolution requests: the host is fixed, or a compiled
%% template rendered per request and checked against the allowed hosts.
%% `pool` is a hackney pool name, or `false` for no connection reuse.
-type one_off_base() :: #{
    scheme := http | https,
    host := {static, binary()} | {template, emqx_template:t(), [allowed_host()]},
    port := inet:port_number(),
    connect_timeout := timeout(),
    ssl_opts := [ssl:tls_client_option()],
    pool := atom() | false
}.

-export_type([allowed_host/0, hostname_resolution/0, url_template/0, one_off_base/0]).

-ifdef(TEST).
%% Internal functions exported for tests in test/emqx_auth_http_utils_tests.erl.
-export([format_url/4]).
-endif.

-define(DEFAULT_HTTP_REQUEST_CONTENT_TYPE, <<"application/json">>).

%% Rendered host must be a plain DNS name or IPv4 address:
%% non-empty labels of [a-z0-9-] separated by dots.
-define(HOSTNAME_RE, "^[a-z0-9-]+(\\.[a-z0-9-]+)*$").
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
Parse a URL whose host part may contain template placeholders (`${...}`).

The returned host is `{template, HostString}` when it contains placeholders
(allowed only with `dynamic` resolution), otherwise `{static, Host}` (IPv6
hosts are formatted with brackets). Only the host may be templated: the
scheme must be literal `http`/`https` and the port, if present, must be a
literal integer. Throws `{invalid_url, Reason, Url}` on invalid URLs and on
a templated host with `static` resolution.
""".
-spec parse_url_template(binary(), hostname_resolution()) -> url_template().
parse_url_template(Url, Resolution) ->
    try
        do_parse_url_template(emqx_utils_uri:parse(Url), Resolution)
    catch
        throw:Reason ->
            throw({invalid_url, Reason, Url})
    end.

do_parse_url_template(#{scheme := undefined}, _Resolution) ->
    throw(no_scheme);
do_parse_url_template(#{authority := undefined}, _Resolution) ->
    throw(no_host);
do_parse_url_template(#{authority := #{userinfo := Userinfo}}, _Resolution) when
    Userinfo =/= undefined
->
    throw(userinfo_not_supported);
do_parse_url_template(#{fragment := Fragment}, _Resolution) when Fragment =/= undefined ->
    throw(fragments_not_supported);
do_parse_url_template(
    #{scheme := SchemeBin, authority := #{host := Host, host_type := HostType, port := Port}} =
        Parsed,
    Resolution
) ->
    Scheme = parse_scheme(SchemeBin),
    #{
        scheme => Scheme,
        host => parse_host(Host, HostType, Resolution),
        port => emqx_maybe:define(Port, default_port(Scheme)),
        path => emqx_utils_uri:path(Parsed),
        query => emqx_maybe:define(emqx_utils_uri:query(Parsed), <<>>)
    }.

parse_host(Host, HostType, Resolution) ->
    case is_templated(Host) of
        false ->
            {static, format_static_host(Host, HostType)};
        true ->
            template_host(Host, HostType, Resolution)
    end.

template_host(Host, regular, dynamic) ->
    {template, Host};
template_host(_Host, _HostType, dynamic) ->
    throw(unsupported_templated_host);
template_host(_Host, _HostType, static) ->
    throw(hostname_resolution_must_be_dynamic).

format_static_host(<<>>, _HostType) ->
    throw(no_host);
format_static_host(Host, regular) ->
    Host;
format_static_host(Host, ipv6) ->
    <<"[", Host/binary, "]">>;
format_static_host(_Host, loose) ->
    throw(invalid_host).

-doc """
Convert a static-host URL template to the request base passed to the
`ehttpc`-based connector.
""".
-spec request_base(url_template()) -> emqx_utils_uri:request_base().
request_base(#{scheme := Scheme, host := {static, Host}, port := Port}) ->
    #{
        scheme => Scheme,
        host => binary_to_list(string:lowercase(unbracket(Host))),
        port => Port
    }.

unbracket(<<"[", Rest/binary>>) ->
    binary:part(Rest, 0, byte_size(Rest) - 1);
unbracket(Host) ->
    Host.

-doc """
Resolve the hackney pool to use for dynamic-resolution requests.
`PoolSize = 0` disables connection reuse entirely; otherwise the named pool is
started (idempotent) with `PoolSize` as its connection cap, updating the cap
if the pool already exists.
""".
-spec ensure_pool(atom(), non_neg_integer()) -> atom() | false.
ensure_pool(_Name, 0) ->
    false;
ensure_pool(Name, PoolSize) ->
    ok = hackney_pool:start_pool(Name, [{max_connections, PoolSize}]),
    ok = hackney_pool:set_max_connections(Name, PoolSize),
    Name.

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

parse_scheme(<<"http">>) -> http;
parse_scheme(<<"https">>) -> https;
parse_scheme(_) -> throw(unsupported_scheme).

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
Issue a per-request (dynamic hostname resolution) HTTP request to either the
fixed host or a per-request rendered host. The request tuple is the one
produced by `generate_request/2`. The result shape is the same as the pooled
(`ehttpc`) query path:
`{ok, Status, Headers} | {ok, Status, Headers, Body} | {error, Reason}`.
""".
-spec one_off_request(one_off_base(), atom(), timeout(), map(), tuple()) ->
    {ok, pos_integer(), list()} | {ok, pos_integer(), list(), binary()} | {error, term()}.
one_off_request(
    #{
        scheme := Scheme,
        port := Port,
        connect_timeout := ConnectTimeout,
        ssl_opts := SslOpts,
        pool := Pool
    } = OneOffBase,
    Method,
    Timeout,
    Values,
    Request
) ->
    maybe
        {ok, Host} ?= resolve_host(OneOffBase, Values),
        Url = format_url(Scheme, Host, Port, request_path_query(Request)),
        ReqOpts = [
            {connect_timeout, ConnectTimeout},
            {recv_timeout, Timeout},
            {pool, Pool}
            | scheme_transport_opts(Scheme, SslOpts)
        ],
        ?MODULE:do_one_off_request(
            Method, Url, request_headers(Request), request_body(Request), ReqOpts
        )
    end.

resolve_host(#{host := {static, Host}}, _Values) ->
    {ok, Host};
resolve_host(#{host := {template, HostTemplate, AllowedHosts}}, Values) ->
    render_host(HostTemplate, AllowedHosts, Values).

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
