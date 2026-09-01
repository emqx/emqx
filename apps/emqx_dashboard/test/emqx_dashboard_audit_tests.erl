%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_audit_tests).

-include_lib("eunit/include/eunit.hrl").

%% Fixed inputs to `emqx_dashboard_audit:log_meta/3' shared by every test
%% case below: a fake cowboy request and an importance level, neither of
%% which any test needs to vary.
%%
%% `log_meta/3' only needs a plain map shaped like a cowboy request (in
%% Cowboy 2.x a request is just a map); this avoids booting the whole
%% app just to exercise the redaction/shape logic below `log_meta/3'.
req() ->
    #{headers => #{}, peer => {{127, 0, 0, 1}, 0}}.

%% Any importance above the "low" threshold (30) skips the
%% `ignore_high_frequency_request/0' branch, which reads `emqx_conf'.
-define(IMPORTANCE, 100).

base_meta() ->
    #{
        code => 204,
        method => put,
        auth_type => jwt_token,
        operation_id => <<"/mqtt/topic_metrics2/:name/reset">>,
        bindings => #{name => <<"test">>},
        headers => #{},
        body => #{},
        req_start => 0,
        req_end => 1000
    }.

log_meta(Meta) ->
    emqx_dashboard_audit:log_meta(?IMPORTANCE, Meta, req()).

http_request(Meta) ->
    #{http_request := Request} = log_meta(Meta),
    Request.

no_query_string_or_namespace_test() ->
    Request = http_request(base_meta()),
    ?assertNot(maps:is_key(query_string, Request)),
    ?assertNot(maps:is_key(namespace, Request)).

empty_query_string_omitted_test() ->
    Request = http_request((base_meta())#{query_string => #{}}),
    ?assertNot(maps:is_key(query_string, Request)).

query_string_is_recorded_test() ->
    Meta = (base_meta())#{query_string => #{<<"ns">> => <<"acme">>}},
    Request = http_request(Meta),
    ?assertEqual(#{<<"ns">> => <<"acme">>}, maps:get(query_string, Request)).

%% A query parameter under a key `emqx_utils_redact' treats as sensitive
%% (e.g. `token') is never written to the audit log in clear, same as it
%% already is for headers and bodies.
query_string_secret_redacted_test() ->
    Meta = (base_meta())#{
        query_string => #{<<"token">> => <<"super-secret">>, <<"ns">> => <<"acme">>}
    },
    Request = http_request(Meta),
    #{query_string := Qs} = Request,
    ?assertEqual(<<"******">>, maps:get(<<"token">>, Qs)),
    ?assertEqual(<<"acme">>, maps:get(<<"ns">>, Qs)).

%% Regression for #18653: a handler that resolves a target namespace and
%% calls `minirest_handler:update_log_meta(#{namespace => Ns})' gets that
%% namespace recorded on the audit record, independent of whether an `ns'
%% query parameter was present at all.
namespace_is_recorded_test() ->
    Meta = (base_meta())#{namespace => <<"acme">>},
    Request = http_request(Meta),
    ?assertEqual(<<"acme">>, maps:get(namespace, Request)).
