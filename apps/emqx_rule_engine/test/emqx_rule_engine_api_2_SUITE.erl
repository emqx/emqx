%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_common_test_http:create_default_app(),
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
        {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
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

do_t_rule_test_smoke(#{input := Input, expected := #{code := ExpectedCode}} = Case) ->
    {_ErrOrOk, {{_, Code, _}, _, Body}} = sql_test_api(Input),
    case Code =:= ExpectedCode of
        true ->
            false;
        false ->
            {true, #{
                expected => ExpectedCode,
                hint => maps:get(hint, Case, <<>>),
                got => Code,
                resp_body => Body
            }}
    end.
