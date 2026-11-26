%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_http_utils_tests).

-include_lib("eunit/include/eunit.hrl").

%% Defense-in-depth: even if some new render path bypasses the PP2 ingestion
%% sanitizer, the HTTP request generator must reject header values containing
%% bytes that would split the request line.
generate_request_rejects_crlf_in_header_test_() ->
    [
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_cr}},
            do_generate(<<"alice\r\nX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_lf}},
            do_generate(<<"alice\nX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_cr}},
            do_generate(<<"alice\rX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_null}},
            do_generate(<<"alice", 0, "X-Override-Result: allow">>)
        )
    ].

generate_request_accepts_clean_header_test() ->
    ?assertMatch(
        {ok, _},
        do_generate(<<"alice.example.com">>)
    ).

do_generate(CN) ->
    {_, HeadersTpl} = emqx_authn_utils:parse_deep(
        maps:to_list(#{
            <<"content-type">> => <<"application/json">>,
            <<"x-cert-cn">> => <<"${cert_common_name}">>
        })
    ),
    {_, BodyTpl} = emqx_authn_utils:parse_deep(#{}),
    {_, QueryTpl} = emqx_authn_utils:parse_deep([]),
    {_, PathTpl} = emqx_authn_utils:parse_str(<<"/auth">>),
    State = #{
        method => post,
        headers_template => HeadersTpl,
        body_template => BodyTpl,
        base_path_template => PathTpl,
        base_query_template => QueryTpl
    },
    emqx_auth_http_utils:generate_request(State, #{cert_common_name => CN}).

-define(DOT_SEGMENT_ERROR, {error, {invalid_url_path, dot_segment}}).

generate_request_rejects_dot_segment_from_value_test_() ->
    Tpl = <<"/auth/${clientid}/check">>,
    [
        ?_assertEqual(?DOT_SEGMENT_ERROR, generate_path(get, Tpl, #{clientid => <<"..">>})),
        ?_assertEqual(?DOT_SEGMENT_ERROR, generate_path(post, Tpl, #{clientid => <<"..">>})),
        ?_assertEqual(?DOT_SEGMENT_ERROR, generate_path(get, Tpl, #{clientid => <<".">>})),
        ?_assertEqual(?DOT_SEGMENT_ERROR, generate_path(post, Tpl, #{clientid => <<".">>})),
        ?_assertEqual(
            ?DOT_SEGMENT_ERROR,
            generate_path(get, <<"/auth/${clientid}">>, #{clientid => <<"..">>})
        ),
        ?_assertEqual(
            ?DOT_SEGMENT_ERROR,
            generate_path(get, <<"/auth/.${clientid}/y">>, #{clientid => <<>>})
        ),
        ?_assertEqual(
            ?DOT_SEGMENT_ERROR,
            generate_path(
                get,
                <<"/auth/${username}${clientid}/x">>,
                #{username => <<".">>, clientid => <<".">>}
            )
        ),
        ?_assertEqual(
            ?DOT_SEGMENT_ERROR,
            generate_path(get, <<"/auth/.${clientid}/x">>, #{clientid => <<".">>})
        )
    ].

generate_request_accepts_dots_inside_segment_test_() ->
    Tpl = <<"/auth/${clientid}/check">>,
    Path = fun(ClientId) -> generate_path(get, Tpl, #{clientid => ClientId}) end,
    [
        ?_assertEqual({ok, <<"/auth/..%2Fx/check">>}, Path(<<"../x">>)),
        ?_assertEqual({ok, <<"/auth/a..b/check">>}, Path(<<"a..b">>)),
        ?_assertEqual({ok, <<"/auth/.../check">>}, Path(<<"...">>)),
        ?_assertEqual({ok, <<"/auth/.hidden/check">>}, Path(<<".hidden">>)),
        ?_assertEqual({ok, <<"/auth/..x/check">>}, Path(<<"..x">>)),
        ?_assertEqual({ok, <<"/auth/%252e%252e/check">>}, Path(<<"%2e%2e">>)),
        ?_assertEqual({ok, <<"/auth/%252E/check">>}, Path(<<"%2E">>)),
        %% Empty segments are kept as they are.
        ?_assertEqual({ok, <<"/auth//check">>}, Path(<<>>)),
        ?_assertEqual({ok, <<"/auth//check">>}, generate_path(post, Tpl, #{clientid => <<>>}))
    ].

generate_request_rejects_static_dot_segment_test_() ->
    Rejected = [
        <<"/a/../b">>,
        <<"/a/./b">>,
        <<"/..">>,
        <<"/a/.">>,
        <<"/a/%2e%2e/b">>,
        <<"/a/%2E%2E/b">>,
        <<"/a/.%2e/b">>,
        <<"/a/%2E./b">>,
        <<"/a/%2e/b">>,
        <<"/a/%2E/b">>,
        <<"../a">>,
        <<".">>,
        <<"..">>,
        <<"/a/..">>,
        <<"/a/%2e">>,
        <<"/a/.%2E">>,
        <<"/a/b/./">>
    ],
    Accepted = [
        <<"">>,
        <<"/">>,
        <<"/a//b/">>,
        <<"/a/.b/c">>,
        <<"/a/b./c">>,
        <<"/a/.../c">>,
        <<"/a/%2e%2e%2e/c">>,
        <<"/a/%2ex/c">>,
        <<"/a/%252e/c">>,
        <<"/a/x.">>,
        <<"/a/.x">>,
        <<"/a/x..">>,
        <<"/a/%2">>,
        <<"/a/%2e%2">>,
        <<"/a/%2f..">>,
        <<"/a/b.c/">>
    ],
    [
        ?_assertEqual(?DOT_SEGMENT_ERROR, generate_path(get, P, #{}))
     || P <- Rejected
    ] ++
        [?_assertEqual({ok, P}, generate_path(get, P, #{})) || P <- Accepted].

generate_request_checks_path_only_test_() ->
    %% A dot in the query string or the body is not a path segment.
    [
        ?_assertMatch(
            {ok, {<<"/auth?clientid=..">>, _}},
            generate(get, <<"/auth">>, [{<<"clientid">>, <<"${clientid}">>}], #{}, #{
                clientid => <<"..">>
            })
        ),
        ?_assertMatch(
            {ok, {<<"/auth">>, _, <<"{\"clientid\":\"..\"}">>}},
            generate(post, <<"/auth">>, [], #{<<"clientid">> => <<"${clientid}">>}, #{
                clientid => <<"..">>
            })
        )
    ].

generate_path(Method, PathTpl, Values) ->
    case generate(Method, PathTpl, [], #{}, Values) of
        {ok, {PathQuery, _Headers}} -> {ok, iolist_to_binary(PathQuery)};
        {ok, {PathQuery, _Headers, _Body}} -> {ok, iolist_to_binary(PathQuery)};
        {error, _} = Error -> Error
    end.

generate(Method, PathTpl0, Query, Body, Values) ->
    {_, HeadersTpl} = emqx_authn_utils:parse_deep(
        maps:to_list(#{<<"content-type">> => <<"application/json">>})
    ),
    {_, BodyTpl} = emqx_authn_utils:parse_deep(Body),
    {_, QueryTpl} = emqx_authn_utils:parse_deep(Query),
    {_, PathTpl} = emqx_authn_utils:parse_str(PathTpl0),
    State = #{
        method => Method,
        headers_template => HeadersTpl,
        body_template => BodyTpl,
        base_path_template => PathTpl,
        base_query_template => QueryTpl
    },
    case emqx_auth_http_utils:generate_request(State, Values) of
        {ok, {PathQuery, Headers}} ->
            {ok, {iolist_to_binary(PathQuery), Headers}};
        {ok, {PathQuery, Headers, ReqBody}} ->
            {ok, {iolist_to_binary(PathQuery), Headers, ReqBody}};
        {error, _} = Error ->
            Error
    end.
