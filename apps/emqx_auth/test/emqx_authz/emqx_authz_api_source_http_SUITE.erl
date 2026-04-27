%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_source_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(SOURCE_HTTP, ?SOURCE_HTTP(#{})).
-define(SOURCE_HTTP(HEADERS), #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://fake.com:443/acl?username=", ?PH_USERNAME/binary>>,
    <<"ssl">> => #{<<"enable">> => true},
    <<"headers">> => HEADERS,
    <<"method">> => <<"get">>,
    <<"request_timeout">> => <<"5s">>
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create_local, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, health_check, fun(St) -> {ok, St} end),
    meck:expect(emqx_resource, remove_local, fun(_) -> ok end),
    meck:expect(
        emqx_authz_file,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_auth, "etc/acl.conf")
        end
    ),

    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf,
                "authorization { cache { enable = false }, no_match = deny, sources = [] }"},
            emqx_auth,
            emqx_auth_http,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    meck:unload(emqx_resource),
    ok.

init_per_testcase(t_api, Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    meck:new(emqx_utils, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_utils, gen_id, fun() -> "fake" end),

    meck:new(emqx, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx,
        data_dir,
        fun() ->
            {data_dir, Data} = lists:keyfind(data_dir, 1, Config),
            Data
        end
    ),
    Config;
init_per_testcase(_, Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    Config.

end_per_testcase(t_api, _Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    meck:unload(emqx_utils),
    meck:unload(emqx),
    ok;
end_per_testcase(_, _Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_http_headers_api(_) ->
    {ok, 204, _} = request(post, uri(["authorization", "sources"]), ?SOURCE_HTTP),

    {ok, 200, Result1} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(
        #{
            <<"type">> := <<"http">>,
            <<"headers">> := M
        } when map_size(M) =:= 0,
        emqx_utils_json:decode(Result1)
    ),

    {ok, 204, _} = request(
        put,
        uri(["authorization", "sources", "http"]),
        ?SOURCE_HTTP(#{<<"a">> => <<"b">>})
    ),

    {ok, 200, Result2} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(
        #{
            <<"type">> := <<"http">>,
            <<"headers">> := #{<<"a">> := <<"b">>}
        },
        emqx_utils_json:decode(Result2)
    ),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "http"]), ?SOURCE_HTTP),

    {ok, 200, Result4} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(
        #{
            <<"type">> := <<"http">>,
            <<"headers">> := M
        } when map_size(M) =:= 0,
        emqx_utils_json:decode(Result4)
    ).

t_http_precondition_api(_) ->
    Precondition1 = <<"str_eq(username, 'u1')">>,
    Source1 = maps:merge(?SOURCE_HTTP, #{<<"precondition">> => Precondition1}),
    {ok, 204, _} = request(post, uri(["authorization", "sources"]), Source1),

    {ok, 200, Result1} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(
        #{
            <<"type">> := <<"http">>,
            <<"precondition">> := Precondition1
        },
        emqx_utils_json:decode(Result1)
    ),
    ?assertMatch(#{type := http, precondition := #{}}, emqx_authz:lookup_state(http)),

    Precondition2 = <<"str_eq(topic, 't/1')">>,
    Source2 = maps:merge(?SOURCE_HTTP, #{<<"precondition">> => Precondition2}),
    {ok, 204, _} = request(put, uri(["authorization", "sources", "http"]), Source2),

    {ok, 200, Result2} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(
        #{
            <<"type">> := <<"http">>,
            <<"precondition">> := Precondition2
        },
        emqx_utils_json:decode(Result2)
    ),

    {ok, 204, _} = request(
        put,
        uri(["authorization", "sources", "http"]),
        maps:merge(?SOURCE_HTTP, #{<<"precondition">> => <<>>})
    ),
    ?assertNot(maps:is_key(precondition, emqx_authz:lookup_state(http))).

t_bad_precondition_api(_) ->
    BadSource = maps:merge(?SOURCE_HTTP, #{<<"precondition">> => <<"not a valid precondition">>}),
    {ok, 400, Result} = request(post, uri(["authorization", "sources"]), BadSource),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := Message
        } when is_binary(Message),
        emqx_utils_json:decode(Result)
    ),
    #{<<"message">> := Message} = emqx_utils_json:decode(Result),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"matched_type">> := <<"authz:http_get">>,
            <<"path">> := <<"root.precondition">>,
            <<"reason">> := #{
                <<"cause">> := <<"bad_precondition_expression">>,
                <<"expression">> := <<"not a valid precondition">>
            }
        },
        emqx_utils_json:decode(Message)
    ),
    ?assertEqual([], emqx_authz:lookup_states()).

data_dir() -> emqx:data_dir().
