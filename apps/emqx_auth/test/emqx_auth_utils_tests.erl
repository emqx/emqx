%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_utils_tests).

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
            {error, {bad_http_header_value, _, contains_nul}},
            do_generate(<<"alice", 0, "X-Override-Result: allow">>)
        )
    ].

generate_request_accepts_clean_header_test() ->
    ?assertMatch(
        {ok, _},
        do_generate(<<"alice.example.com">>)
    ).

do_generate(CN) ->
    HeadersTpl = emqx_authn_utils:parse_deep(
        maps:to_list(#{
            <<"content-type">> => <<"application/json">>,
            <<"x-cert-cn">> => <<"${cert_common_name}">>
        })
    ),
    BodyTpl = emqx_authn_utils:parse_deep(#{}),
    QueryTpl = emqx_authn_utils:parse_deep([]),
    PathTpl = emqx_authn_utils:parse_str(<<"/auth">>),
    State = #{
        method => post,
        headers => HeadersTpl,
        body_template => BodyTpl,
        base_path_template => PathTpl,
        base_query_template => QueryTpl
    },
    emqx_auth_utils:generate_request(State, #{cert_common_name => CN}).
