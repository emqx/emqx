%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_rule_engine_api_2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

app_specs() ->
    [
        emqx_conf,
        emqx_rule_engine,
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard()
    ].

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    Opts = #{return_all => true},
    request(Method, Path, Params, Opts).

request(Method, Path, Params, Opts) ->
    request(Method, Path, Params, _QueryParams = [], Opts).

request(Method, Path, Params, QueryParams0, Opts) when is_list(QueryParams0) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    QueryParams = uri_string:compose_query(QueryParams0, [{encoding, utf8}]),
    case emqx_mgmt_api_test_util:request_api(Method, Path, QueryParams, AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

sql_test_api(Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rule_test"]),
    ct:pal("sql test (http):\n  ~p", [Params]),
    Res = request(Method, Path, Params),
    ct:pal("sql test (http) result:\n  ~p", [Res]),
    Res.

list_rules(QueryParams) when is_list(QueryParams) ->
    Method = get,
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Opts = #{return_all => true},
    Res = request(Method, Path, _Body = [], QueryParams, Opts),
    emqx_mgmt_api_test_util:simplify_result(Res).

list_rules_just_ids(QueryParams) when is_list(QueryParams) ->
    case list_rules(QueryParams) of
        {200, #{<<"data">> := Results0}} ->
            Results = lists:sort([Id || #{<<"id">> := Id} <- Results0]),
            {200, Results};
        Res ->
            Res
    end.

create_rule() ->
    create_rule(_Overrides = #{}).

create_rule(Overrides) ->
    Params0 = #{
        <<"enable">> => true,
        <<"sql">> => <<"select true from t">>
    },
    Params = emqx_utils_maps:deep_merge(Params0, Overrides),
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Res = request(Method, Path, Params),
    case emqx_mgmt_api_test_util:simplify_result(Res) of
        {201, #{<<"id">> := RuleId}} = SRes ->
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            SRes;
        SRes ->
            SRes
    end.

sources_sql(Sources) ->
    Froms = iolist_to_binary(lists:join(<<", ">>, lists:map(fun source_from/1, Sources))),
    <<"select * from ", Froms/binary>>.

source_from({v1, Id}) ->
    <<"\"$bridges/", Id/binary, "\" ">>;
source_from({v2, Id}) ->
    <<"\"$sources/", Id/binary, "\" ">>.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_rule_test_smoke(_Config) ->
    %% Example inputs recorded from frontend on 2023-12-04
    Publish = [
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            hint => <<"wrong topic">>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            hint => <<
                "Currently, the frontend doesn't try to match against "
                "$events/message_published, but it may start sending "
                "the event topic in the future."
            >>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"$events/message_published\"">>
                }
        }
    ],
    %% Default input SQL doesn't match any event topic
    DefaultNoMatch = [
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"message_delivered">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"message_acked">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_dropped">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"reason">> => <<"no_subscribers">>,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_connected">>,
                            <<"peername">> => <<"127.0.0.1:52918">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_disconnected">>,
                            <<"reason">> => <<"normal">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_connack">>,
                            <<"reason_code">> => <<"success">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"action">> => <<"publish">>,
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authz_complete">>,
                            <<"result">> => <<"allow">>,
                            <<"topic">> => <<"t/1">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authn_complete">>,
                            <<"reason_code">> => <<"success">>,
                            <<"is_superuser">> => true,
                            <<"is_anonymous">> => false,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authn_complete">>,
                            <<"reason_code">> => <<"sucess">>,
                            <<"is_superuser">> => true,
                            <<"is_anonymous">> => false,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_subscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"delivery_dropped">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"reason">> => <<"queue_full">>,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        }
    ],
    MultipleFrom = [
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$bridges/mqtt:source\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$sources/mqtt:source\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$events/session_unsubscribed\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/message_dropped\", \"$events/session_unsubscribed\" ">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/message_dropped\", \"$events/client_connected\" ">>
                }
        }
    ],
    Cases = Publish ++ DefaultNoMatch ++ MultipleFrom,
    FailedCases = lists:filtermap(fun do_t_rule_test_smoke/1, Cases),
    ?assertEqual([], FailedCases),
    ok.

%% validate check_schema is function with bad content_type
t_rule_test_with_bad_content_type(_Config) ->
    Params =
        #{
            <<"context">> =>
                #{
                    <<"clientid">> => <<"c_emqx">>,
                    <<"event_type">> => <<"message_publish">>,
                    <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                    <<"qos">> => 1,
                    <<"topic">> => <<"t/a">>,
                    <<"username">> => <<"u_emqx">>
                },
            <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
        },
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rule_test"]),
    Opts = #{return_all => true, 'content-type' => "application/xml"},
    ?assertMatch(
        {error,
            {
                {"HTTP/1.1", 415, "Unsupported Media Type"},
                _Headers,
                #{
                    <<"code">> := <<"UNSUPPORTED_MEDIA_TYPE">>,
                    <<"message">> := <<"content-type:application/json Required">>
                }
            }},
        request(Method, Path, Params, Opts)
    ),
    ok.

do_t_rule_test_smoke(#{input := Input, expected := #{code := ExpectedCode}} = Case) ->
    {_ErrOrOk, {{_, Code, _}, _, Body}} = sql_test_api(Input),
    case Code =:= ExpectedCode of
        true ->
            false;
        false ->
            {true, #{
                expected => ExpectedCode,
                hint => maps:get(hint, Case, <<>>),
                input => Input,
                got => Code,
                resp_body => Body
            }}
    end.

%% Tests filtering the rule list by used actions and/or sources.
t_filter_by_source_and_action(_Config) ->
    ?assertMatch(
        {200, #{<<"data">> := []}},
        list_rules([])
    ),

    ActionId1 = <<"mqtt:a1">>,
    ActionId2 = <<"mqtt:a2">>,
    SourceId1 = <<"mqtt:s1">>,
    SourceId2 = <<"mqtt:s2">>,
    {201, #{<<"id">> := Id1}} = create_rule(#{<<"actions">> => [ActionId1]}),
    {201, #{<<"id">> := Id2}} = create_rule(#{<<"actions">> => [ActionId2]}),
    {201, #{<<"id">> := Id3}} = create_rule(#{<<"actions">> => [ActionId2, ActionId1]}),
    {201, #{<<"id">> := Id4}} = create_rule(#{<<"sql">> => sources_sql([{v1, SourceId1}])}),
    {201, #{<<"id">> := Id5}} = create_rule(#{<<"sql">> => sources_sql([{v2, SourceId2}])}),
    {201, #{<<"id">> := Id6}} = create_rule(#{
        <<"sql">> => sources_sql([{v2, SourceId1}, {v2, SourceId1}])
    }),
    {201, #{<<"id">> := Id7}} = create_rule(#{
        <<"sql">> => sources_sql([{v2, SourceId1}]),
        <<"actions">> => [ActionId1]
    }),

    ?assertMatch(
        {200, [_, _, _, _, _, _, _]},
        list_rules_just_ids([])
    ),

    ?assertEqual(
        {200, lists:sort([Id1, Id3, Id7])},
        list_rules_just_ids([{<<"action">>, ActionId1}])
    ),

    ?assertEqual(
        {200, lists:sort([Id1, Id2, Id3, Id7])},
        list_rules_just_ids([{<<"action">>, ActionId1}, {<<"action">>, ActionId2}])
    ),

    ?assertEqual(
        {200, lists:sort([Id4, Id6, Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}])
    ),

    ?assertEqual(
        {200, lists:sort([Id4, Id5, Id6, Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}, {<<"source">>, SourceId2}])
    ),

    %% When mixing source and action id filters, we use AND.
    ?assertEqual(
        {200, lists:sort([])},
        list_rules_just_ids([{<<"source">>, SourceId2}, {<<"action">>, ActionId2}])
    ),
    ?assertEqual(
        {200, lists:sort([Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}, {<<"action">>, ActionId1}])
    ),

    ok.

%% Checks that creating a rule with a `null' JSON value id is forbidden.
t_create_rule_with_null_id(_Config) ->
    ?assertMatch(
        {400, #{<<"message">> := <<"rule id must be a string">>}},
        create_rule(#{<<"id">> => null})
    ),
    %% The string `"null"' should be fine.
    ?assertMatch(
        {201, _},
        create_rule(#{<<"id">> => <<"null">>})
    ),
    ?assertMatch({201, _}, create_rule(#{})),
    ?assertMatch(
        {200, #{<<"data">> := [_, _]}},
        list_rules([])
    ),
    ok.
