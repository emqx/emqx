%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module provides a loose parser for URIs.
%% The standard library's `uri_string' module is strict and does not allow
%% to parse invalid URIs, like templates: `http://example.com/${username}'.

-module(emqx_utils_uri).

-export([parse/1, format/1]).

-export([
    scheme/1,
    userinfo/1,
    host/1,
    port/1,
    path/1,
    query/1,
    fragment/1,
    base_url/1,
    request_base/1
]).

-type scheme() :: binary().
-type userinfo() :: binary().
-type host() :: binary().
-type port_number() :: inet:port_number().
-type path() :: binary().
-type query() :: binary().
-type fragment() :: binary().
-type request_base() :: #{
    scheme := http | https,
    host := iolist(),
    port := inet:port_number()
}.

-type authority() :: #{
    userinfo := emqx_maybe:t(userinfo()),
    host := host(),
    %% Types:
    %% ipv6: `\[[a-z\d:\.]*\]` — bracketed "ivp6-like" address
    %% regular: `example.com` — arbitrary host not containg `:` which is forbidden in hostnames other than ipv6
    %% loose: non ipv6-like host containing `:`, probably invalid for a strictly valid URI
    host_type := ipv6 | regular | loose,
    port := emqx_maybe:t(port_number())
}.

-type uri() :: #{
    scheme := emqx_maybe:t(scheme()),
    authority := emqx_maybe:t(authority()),
    path := path(),
    query := emqx_maybe:t(query()),
    fragment := emqx_maybe:t(fragment())
}.

-export_type([
    scheme/0,
    userinfo/0,
    host/0,
    port_number/0,
    path/0,
    query/0,
    fragment/0,
    authority/0,
    uri/0,
    request_base/0
]).

-on_load(init/0).

%% https://datatracker.ietf.org/doc/html/rfc3986#appendix-B
%%
%% > The following line is the regular expression for breaking-down a
%% > well-formed URI reference into its components.
%%
%% > ^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?
%%
%% We skip capturing some unused parts of the regex.

-define(URI_REGEX,
    ("^(?:(?<scheme>[^:/?#]+):)?(?<authority>//[^/?#]*)?"
    "(?<path>[^?#]*)(?<query>\\?[^#]*)?(?<fragment>#.*)?")
).

-define(URI_REGEX_PT_KEY, {?MODULE, uri_re}).

-define(AUTHORITY_REGEX,
    ("^(?<userinfo>.*@)?"
    "(?:(?:\\[(?<host_ipv6>[a-z\\d\\.:]*)\\])|(?<host_regular>[^:]*?)|(?<host_loose>.*?))"
    "(?<port>:\\d+)?$")
).

-define(AUTHORITY_REGEX_PT_KEY, {?MODULE, authority_re}).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

init() ->
    {ok, UriRE} = re:compile(?URI_REGEX),
    persistent_term:put(?URI_REGEX_PT_KEY, UriRE),

    {ok, AuthorityRE} = re:compile(?AUTHORITY_REGEX, [caseless]),
    persistent_term:put(?AUTHORITY_REGEX_PT_KEY, AuthorityRE).

%%-------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------

-spec parse(binary()) -> uri().
parse(URIString) ->
    {match, [SchemeMatch, AuthorityMatch, PathMatch, QueryMatch, FragmentMatch]} = re:run(
        URIString, uri_regexp(), [{capture, [scheme, authority, path, query, fragment], binary}]
    ),
    Scheme = parse_scheme(SchemeMatch),
    Authority = parse_authority(AuthorityMatch),
    Path = PathMatch,
    Query = parse_query(QueryMatch),
    Fragment = parse_fragment(FragmentMatch),

    #{
        scheme => Scheme,
        authority => Authority,
        path => Path,
        query => Query,
        fragment => Fragment
    }.

-spec base_url(uri()) -> iodata().
base_url(#{scheme := Scheme, authority := Authority}) ->
    [format_scheme(Scheme), format_authority(Authority)].

-spec format(uri()) -> iodata().
format(#{path := Path, query := Query, fragment := Fragment} = URI) ->
    [
        base_url(URI),
        Path,
        format_query(Query),
        format_fragment(Fragment)
    ].

-spec scheme(uri()) -> emqx_maybe:t(scheme()).
scheme(#{scheme := Scheme}) -> Scheme.

-spec userinfo(uri()) -> emqx_maybe:t(userinfo()).
userinfo(#{authority := undefined}) -> undefined;
userinfo(#{authority := #{userinfo := UserInfo}}) -> UserInfo.

-spec host(uri()) -> emqx_maybe:t(host()).
host(#{authority := undefined}) -> undefined;
host(#{authority := #{host := Host}}) -> Host.

-spec port(uri()) -> emqx_maybe:t(port_number()).
port(#{authority := undefined}) -> undefined;
port(#{authority := #{port := Port}}) -> Port.

-spec path(uri()) -> path().
path(#{path := Path}) -> Path.

-spec query(uri()) -> emqx_maybe:t(query()).
query(#{query := Query}) -> Query.

-spec fragment(uri()) -> emqx_maybe:t(fragment()).
fragment(#{fragment := Fragment}) -> Fragment.

-spec request_base(uri()) -> {ok, request_base()} | {error, term()}.
request_base(URI) when is_map(URI) ->
    case emqx_http_lib:uri_parse(iolist_to_binary(base_url(URI))) of
        {error, Reason} -> {error, Reason};
        {ok, URIMap} -> {ok, maps:with([scheme, host, port], URIMap)}
    end;
request_base(URIString) when is_list(URIString) orelse is_binary(URIString) ->
    request_base(parse(URIString)).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

parse_scheme(<<>>) -> undefined;
parse_scheme(Scheme) -> Scheme.

parse_query(<<>>) -> undefined;
parse_query(<<$?, Query/binary>>) -> Query.

parse_fragment(<<>>) -> undefined;
parse_fragment(<<$#, Fragment/binary>>) -> Fragment.

authority_regexp() ->
    persistent_term:get(?AUTHORITY_REGEX_PT_KEY).

parse_authority(<<>>) ->
    undefined;
parse_authority(<<$/, $/, Authority/binary>>) ->
    %% Authority regexp always matches
    {match, [UserInfoMatch, HostIPv6, HostRegular, HostLoose, PortMatch]} = re:run(
        Authority, authority_regexp(), [
            {capture, [userinfo, host_ipv6, host_regular, host_loose, port], binary}
        ]
    ),
    UserInfo = parse_userinfo(UserInfoMatch),
    {HostType, Host} = parse_host(HostIPv6, HostRegular, HostLoose),
    Port = parse_port(PortMatch),
    #{
        userinfo => UserInfo,
        host => Host,
        host_type => HostType,
        port => Port
    }.

parse_userinfo(<<>>) -> undefined;
parse_userinfo(UserInfoMatch) -> binary:part(UserInfoMatch, 0, byte_size(UserInfoMatch) - 1).

parse_host(<<>>, <<>>, Host) -> {loose, Host};
parse_host(<<>>, Host, <<>>) -> {regular, Host};
parse_host(Host, <<>>, <<>>) -> {ipv6, Host}.

parse_port(<<>>) -> undefined;
parse_port(<<$:, Port/binary>>) -> binary_to_integer(Port).

uri_regexp() ->
    persistent_term:get(?URI_REGEX_PT_KEY).

format_scheme(undefined) -> <<>>;
format_scheme(Scheme) -> [Scheme, $:].

format_authority(undefined) ->
    <<>>;
format_authority(#{userinfo := UserInfo, host := Host, host_type := HostType, port := Port}) ->
    [$/, $/, format_userinfo(UserInfo), format_host(HostType, Host), format_port(Port)].

format_userinfo(undefined) -> <<>>;
format_userinfo(UserInfo) -> [UserInfo, $@].

format_host(ipv6, Host) -> [$[, Host, $]];
format_host(_, Host) -> Host.

format_port(undefined) -> <<>>;
format_port(Port) -> [$:, integer_to_binary(Port)].

format_query(undefined) -> <<>>;
format_query(Query) -> [$?, Query].

format_fragment(undefined) -> <<>>;
format_fragment(Fragment) -> [$#, Fragment].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(URLS, [
    "https://www.example.com/page",
    "http://subdomain.example.com/path/to/page",
    "https://www.example.com:8080/path/to/page",
    "https://user:password@example.com/path/to/page",
    "https://www.example.com/path%20with%20${spaces}",
    "http://192.0.2.1/path/to/page",
    "http://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]/${path}/to/page",
    "http://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]/to/page",
    "http://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4444/to/page",
    "ftp://ftp.example.com/${path}/to/file",
    "ftps://ftp.example.com/path/to/file",
    "mailto:user@example.com",
    "tel:+1234567890",
    "sms:+1234567890?body=Hello%20World",
    "git://github.com/user/repo.git",
    "a:b:c",
    "svn://svn.example.com/project/trunk",
    "https://www.${example}.com/path/to/page?query_param=value",
    "https://www.example.com/path/to/page?query_param1=value1&query_param2=value2",
    "https://www.example.com?query_param1=value1&query_param2=value2",
    "https://www.example.com/path/to/page#section1",
    "https://www.example.com/path/to/page?query_param=value#section1",
    "https://www.example.com/path/to/page?query_param1=value1&query_param2=${value2}#section1",
    "https://www.example.com?query_param1=value1&query_param2=value2#section1",
    "file:///path/to/file.txt",
    "localhost",
    "localhost:8080",
    "localhost:8080/path/to/page",
    "localhost:8080/path/to/page?query_param=value",
    "localhost:8080/path/to/page?query_param1=value1&query_param2=value2",
    "/abc/${def}",
    "/abc/def?query_param=value",
    "?query_param=value",
    "#section1"
]).

parse_format_test_() ->
    [
        {URI, ?_assertEqual(list_to_binary(URI), iolist_to_binary(format(parse(URI))))}
     || URI <- ?URLS
    ].

base_url_test_() ->
    [
        {URI, ?_assert(is_prefix(iolist_to_binary(base_url(parse(URI))), list_to_binary(URI)))}
     || URI <- ?URLS
    ].

scheme_test_() ->
    [
        if_parseable_by_uri_string(URI, fun(Expected, Parsed) ->
            ?assertEqual(maybe_get_bin(scheme, Expected), scheme(Parsed))
        end)
     || URI <- ?URLS
    ].

host_test_() ->
    [
        if_parseable_by_uri_string(URI, fun(Expected, Parsed) ->
            ?assertEqual(maybe_get_bin(host, Expected), host(Parsed))
        end)
     || URI <- ?URLS
    ].

path_test_() ->
    [
        if_parseable_by_uri_string(URI, fun(Expected, Parsed) ->
            ?assertEqual(maybe_get_bin(path, Expected), path(Parsed))
        end)
     || URI <- ?URLS
    ].

query_test_() ->
    [
        if_parseable_by_uri_string(URI, fun(Expected, Parsed) ->
            ?assertEqual(maybe_get_bin(query, Expected), query(Parsed))
        end)
     || URI <- ?URLS
    ].

fragment_test_() ->
    [
        if_parseable_by_uri_string(URI, fun(Expected, Parsed) ->
            ?assertEqual(maybe_get_bin(fragment, Expected), fragment(Parsed))
        end)
     || URI <- ?URLS
    ].

templates_test_() ->
    [
        {"template in path",
            ?_assertEqual(
                <<"/${client_attrs.group}">>,
                path(parse("https://www.example.com/${client_attrs.group}"))
            )},
        {"template in query, no path",
            ?_assertEqual(
                <<"group=${client_attrs.group}">>,
                query(parse("https://www.example.com?group=${client_attrs.group}"))
            )},
        {"template in query, path",
            ?_assertEqual(
                <<"group=${client_attrs.group}">>,
                query(parse("https://www.example.com/path/?group=${client_attrs.group}"))
            )}
    ].

request_target_test_() ->
    [
        ?_assertEqual(
            {ok, #{port => 443, scheme => https, host => "www.example.com"}},
            request_base(parse("https://www.example.com/path/to/page?query_param=value#fr"))
        ),
        ?_assertEqual(
            {error, empty_host_not_allowed},
            request_base(parse("localhost?query_param=value#fr"))
        ),
        ?_assertEqual(
            {error, {unsupported_scheme, <<"ftp">>}},
            request_base(parse("ftp://localhost"))
        )
    ].

is_prefix(Prefix, Binary) ->
    case Binary of
        <<Prefix:(byte_size(Prefix))/binary, _/binary>> -> true;
        _ -> false
    end.

if_parseable_by_uri_string(URI, Fun) ->
    case uri_string:parse(URI) of
        {error, _, _} ->
            {"skipped", fun() -> true end};
        ExpectedMap ->
            ParsedMap = parse(URI),
            {URI, fun() -> Fun(ExpectedMap, ParsedMap) end}
    end.

maybe_get_bin(Key, Map) ->
    maybe_bin(maps:get(Key, Map, undefined)).

maybe_bin(String) when is_list(String) -> list_to_binary(String);
maybe_bin(undefined) -> undefined.

-endif.
