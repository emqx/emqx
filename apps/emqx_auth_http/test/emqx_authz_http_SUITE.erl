%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HTTP_PORT, 33333).
-define(HTTP_PATH, "/authz/[...]").

-define(AUTHZ_HTTP_RESP(Result, Req),
    cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        "{\"result\": \"" ++ atom_to_list(Result) ++ "\"}",
        Req
    )
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx_auth,
            emqx_auth_http
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, _Config)).

init_per_testcase(t_bad_response = TestCase, Config) ->
    TCApps = emqx_cth_suite:start_apps(
        [emqx_management, emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    init_per_testcase(common, [{tc_apps, TCApps} | Config]);
init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    {ok, _} = emqx_authz_http_test_server:start_link(?HTTP_PORT, ?HTTP_PATH),
    Config.

end_per_testcase(t_bad_response, Config) ->
    TCApps = ?config(tc_apps, Config),
    emqx_cth_suite:stop_apps(TCApps),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, _Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, true),
    ok = emqx_authz_test_lib:enable_node_cache(false),
    try
        ok = emqx_authz_http_test_server:stop()
    catch
        exit:noproc ->
            ok
    end,
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_response_handling(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    %% OK, get, body & headers
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{}
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, no body
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    deny = emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>),

    %% OK, get, 204
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            Req = cowboy_req:reply(204, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, 400
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            Req = cowboy_req:reply(400, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, 400 + body & headers
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"text/plain">>},
                "Response body",
                Req0
            ),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% the server cannot be reached; should skip to the next
    %% authorizer in the chain.
    ok = emqx_authz_http_test_server:stop(),

    ?check_trace(
        ?assertEqual(
            deny,
            emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
        ),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        ?snk_kind := authz_http_request_failure,
                        error := {recoverable_error, econnrefused}
                    }
                ],
                ?of_kind(authz_http_request_failure, Trace)
            ),
            ?assert(
                ?strict_causality(
                    #{?snk_kind := authz_http_request_failure},
                    #{?snk_kind := authz_non_superuser, result := nomatch},
                    Trace
                )
            ),
            ok
        end
    ),

    ok.

t_query_params(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            #{
                username := <<"user name">>,
                clientid := <<"client id">>,
                peerhost := <<"127.0.0.1">>,
                proto_name := <<"MQTT">>,
                mountpoint := <<"MOUNTPOINT">>,
                topic := <<"t/1">>,
                action := <<"publish">>,
                access := <<"2">>,
                qos := <<"1">>,
                retain := <<"false">>
            } = cowboy_req:match_qs(
                [
                    username,
                    clientid,
                    peerhost,
                    proto_name,
                    mountpoint,
                    topic,
                    action,
                    access,
                    qos,
                    retain
                ],
                Req0
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> => <<
                "http://127.0.0.1:33333/authz/users/?"
                "username=${username}&"
                "clientid=${clientid}&"
                "peerhost=${peerhost}&"
                "proto_name=${proto_name}&"
                "mountpoint=${mountpoint}&"
                "topic=${topic}&"
                "action=${action}&"
                "access=${access}&"
                "qos=${qos}&"
                "retain=${retain}"
            >>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t/1">>)
    ).

t_path(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<
                    "/authz/use+rs/"
                    "user+name/"
                    "client+id/"
                    "127.0.0.1/"
                    "MQTT/"
                    "MOUNTPOINT/"
                    "t%2F1/"
                    "publish/"
                    "2/"
                    "1/"
                    "false"
                >>,
                cowboy_req:path(Req0)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> => <<
                "http://127.0.0.1:33333/authz/use+rs/"
                "${username}/"
                "${clientid}/"
                "${peerhost}/"
                "${proto_name}/"
                "${mountpoint}/"
                "${topic}/"
                "${action}/"
                "${access}/"
                "${qos}/"
                "${retain}"
            >>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t/1">>)
    ).

t_json_body(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),

            ?assertMatch(
                #{
                    <<"username">> := <<"user name">>,
                    <<"CLIENT">> := <<"client id">>,
                    <<"peerhost">> := <<"127.0.0.1">>,
                    <<"proto_name">> := <<"MQTT">>,
                    <<"mountpoint">> := <<"MOUNTPOINT">>,
                    <<"topic">> := <<"t">>,
                    <<"action">> := <<"publish">>,
                    <<"access">> := <<"2">>,
                    <<"qos">> := <<"1">>,
                    <<"retain">> := <<"false">>
                },
                emqx_utils_json:decode(RawBody, [return_maps])
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>,
                <<"CLIENT">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>,
                <<"proto_name">> => <<"${proto_name}">>,
                <<"mountpoint">> => <<"${mountpoint}">>,
                <<"topic">> => <<"${topic}">>,
                <<"action">> => <<"${action}">>,
                <<"access">> => <<"${access}">>,
                <<"qos">> => <<"${qos}">>,
                <<"retain">> => <<"${retain}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>)
    ).

t_no_rich_actions(_Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, false),

    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),

            %% No interpolation if rich_actions is disabled
            ?assertMatch(
                #{
                    <<"qos">> := <<"${qos}">>,
                    <<"retain">> := <<"${retain}">>
                },
                emqx_utils_json:decode(RawBody, [return_maps])
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"qos">> => <<"${qos}">>,
                <<"retain">> => <<"${retain}">>
            }
        }
    ),

    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>)
    ).

t_placeholder_and_body(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

            ?assertMatch(
                #{
                    <<"username">> := <<"user name">>,
                    <<"clientid">> := <<"client id">>,
                    <<"peerhost">> := <<"127.0.0.1">>,
                    <<"proto_name">> := <<"MQTT">>,
                    <<"mountpoint">> := <<"MOUNTPOINT">>,
                    <<"topic">> := <<"t">>,
                    <<"action">> := <<"publish">>,
                    <<"access">> := <<"2">>,
                    <<"CN">> := ?PH_CERT_CN_NAME,
                    <<"CS">> := ?PH_CERT_SUBJECT
                },
                maps:from_list(PostVars)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>,
                <<"clientid">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>,
                <<"proto_name">> => <<"${proto_name}">>,
                <<"mountpoint">> => <<"${mountpoint}">>,
                <<"topic">> => <<"${topic}">>,
                <<"action">> => <<"${action}">>,
                <<"access">> => <<"${access}">>,
                <<"CN">> => ?PH_CERT_CN_NAME,
                <<"CS">> => ?PH_CERT_SUBJECT
            },
            <<"headers">> => #{<<"content-type">> => <<"application/x-www-form-urlencoded">>}
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default},
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

%% Checks that we don't crash when receiving an unsupported content-type back.
t_bad_response_content_type(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, _PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"text/csv">>},
                "hi",
                Req1
            ),
            {ok, Req, State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>,
                <<"clientid">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>,
                <<"proto_name">> => <<"${proto_name}">>,
                <<"mountpoint">> => <<"${mountpoint}">>,
                <<"topic">> => <<"${topic}">>,
                <<"action">> => <<"${action}">>,
                <<"access">> => <<"${access}">>,
                <<"CN">> => ?PH_CERT_CN_NAME,
                <<"CS">> => ?PH_CERT_SUBJECT
            },
            <<"headers">> => #{
                <<"accept">> => <<"text/plain">>,
                <<"content-type">> => <<"application/json">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default},
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },

    ?check_trace(
        ?assertEqual(
            deny,
            emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := <<"unsupported content-type", _/binary>>}],
                ?of_kind(bad_authz_http_response, Trace)
            ),
            ok
        end
    ).

%% Checks that we bump the correct metrics when we receive an error response
t_bad_response(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, _PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

            Req = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"application/json">>},
                "{\"error\":true}",
                Req1
            ),
            {ok, Req, State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>
            },
            <<"headers">> => #{}
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => {tcp, default},
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertMatch(
        #{
            counters := #{
                total := 1,
                ignore := 1,
                nomatch := 0,
                allow := 0,
                deny := 0
            },
            'authorization.superuser' := 0,
            'authorization.matched.allow' := 0,
            'authorization.matched.deny' := 0,
            'authorization.nomatch' := 1
        },
        get_metrics()
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"ignore">> := 1,
                <<"nomatch">> := 0,
                <<"allow">> := 0,
                <<"deny">> := 0,
                <<"total">> := 1
            },
            <<"node_metrics">> := [
                #{
                    <<"metrics">> := #{
                        <<"ignore">> := 1,
                        <<"nomatch">> := 0,
                        <<"allow">> := 0,
                        <<"deny">> := 0,
                        <<"total">> := 1
                    }
                }
            ]
        }},
        get_status_api()
    ),
    ok.

t_no_value_for_placeholder(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),

            ?assertMatch(
                #{
                    <<"mountpoint">> := <<"[]">>
                },
                emqx_utils_json:decode(RawBody, [return_maps])
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"mountpoint">> => <<"[${mountpoint}]">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_node_cache(_Config) ->
    ok = setup_handler_and_config(
        fun(#{path := Path} = Req, State) ->
            case {Path, cowboy_req:match_qs([username, cn], Req)} of
                {<<"/authz/clientid">>, #{username := <<"username">>, cn := <<"cn">>}} ->
                    {ok, ?AUTHZ_HTTP_RESP(allow, Req), State};
                _ ->
                    {ok, ?AUTHZ_HTTP_RESP(deny, Req), State}
            end
        end,
        #{
            <<"method">> => <<"get">>,
            <<"url">> => <<"http://127.0.0.1:33333/authz/${clientid}?username=${username}">>,
            <<"body">> => #{<<"cn">> => <<"${cert_common_name}">>}
        }
    ),
    ok = emqx_authz_test_lib:enable_node_cache(true),

    %% We authorize twice, the second time should be cached
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => {tcp, default},
        cn => <<"cn">>,
        dn => <<"dn">>
    },
    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ),
    %% Now change a var in each interpolated part, the cache should NOT be hit
    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo#{cn => <<"cn2">>}, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(
            ClientInfo#{clientid => <<"clientid2">>}, ?AUTHZ_PUBLISH, <<"t">>
        )
    ),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(
            ClientInfo#{username => <<"username2">>}, ?AUTHZ_PUBLISH, <<"t">>
        )
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 4}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ).

t_disallowed_placeholders_preserved(_Config) ->
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            {ok, Body, Req1} = cowboy_req:read_body(Req0),
            ?assertMatch(
                #{
                    <<"cname">> := <<>>,
                    <<"usertypo">> := <<"${usertypo}">>
                },
                emqx_utils_json:decode(Body)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"cname">> => ?PH_CERT_CN_NAME,
                <<"usertypo">> => <<"${usertypo}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_disallowed_placeholders_path(_Config) ->
    ok = setup_handler_and_config(
        fun(Req, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req), State}
        end,
        #{
            <<"url">> => <<"http://127.0.0.1:33333/authz/use+rs/${typo}">>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => {tcp, default}
    },

    % % NOTE: disallowed placeholder left intact, which makes the URL invalid
    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_create_replace(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ValidConfig = raw_http_authz_config(),

    %% Create with valid URL
    ok = setup_handler_and_config(
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> =>
                <<"http://127.0.0.1:33333/authz/users/?topic=${topic}&action=${action}">>
        }
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Changing to valid config
    OkConfig = ValidConfig#{
        <<"url">> =>
            <<"http://127.0.0.1:33333/authz/users/?topic=${topic}&action=${action}">>
    },

    ?assertMatch(
        {ok, _},
        emqx_authz:update({?CMD_REPLACE, http}, OkConfig)
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> => <<"localhost">>
        })
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> => <<"//foo.bar/x/y?q=z">>
        })
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> =>
                <<"http://127.0.0.1:33333/authz/users/?topic=${topic}&action=${action}#fragment">>
        })
    ).

t_uri_normalization(_Config) ->
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(),
        #{
            <<"url">> =>
                <<"http://127.0.0.1:33333?topic=${topic}&action=${action}">>
        }
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_http_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:33333/authz/users/?topic=${topic}&action=${action}">>,
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

setup_handler_and_config(Handler, Config) ->
    ok = emqx_authz_http_test_server:set_handler(Handler),
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(),
        Config
    ).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).

get_metrics() ->
    Metrics = emqx_metrics_worker:get_metrics(authz_metrics, http),
    lists:foldl(
        fun(Name, Acc) ->
            Acc#{Name => emqx_metrics:val(Name)}
        end,
        Metrics,
        [
            'authorization.superuser',
            'authorization.matched.allow',
            'authorization.matched.deny',
            'authorization.nomatch'
        ]
    ).

get_status_api() ->
    Path = emqx_mgmt_api_test_util:uri(["authorization", "sources", "http", "status"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    Res0 = emqx_mgmt_api_test_util:request_api(get, Path, _QParams = [], Auth, _Body = [], Opts),
    {Status, RawBody} = emqx_mgmt_api_test_util:simplify_result(Res0),
    {Status, emqx_utils_json:decode(RawBody, [return_maps])}.
