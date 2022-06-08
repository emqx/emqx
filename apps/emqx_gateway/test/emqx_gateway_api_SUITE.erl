%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_confs/2,
        assert_feilds_apperence/2,
        request/2,
        request/3,
        ssl_server_opts/0,
        ssl_client_opts/0
    ]
).

-include_lib("eunit/include/eunit.hrl").

%% this parses to #{}, will not cause config cleanup
%% so we will need call emqx_config:erase
-define(CONF_DEFAULT, <<"gateway {}">>).

%%--------------------------------------------------------------------
%% Setup
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    application:load(emqx),
    emqx_config:delete_override_conf_files(),
    emqx_config:erase(gateway),
    emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_authn, emqx_gateway]),
    Conf.

end_per_suite(Conf) ->
    emqx_mgmt_api_test_util:end_suite([emqx_gateway, emqx_authn, emqx_conf]),
    Conf.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_gateway(_) ->
    {200, Gateways} = request(get, "/gateway"),
    lists:foreach(fun assert_gw_unloaded/1, Gateways),
    {400, BadReq} = request(get, "/gateway/uname_gateway"),
    assert_bad_request(BadReq),
    {201, _} = request(post, "/gateway", #{name => <<"stomp">>}),
    {200, StompGw1} = request(get, "/gateway/stomp"),
    assert_feilds_apperence(
        [name, status, enable, created_at, started_at],
        StompGw1
    ),
    {204, _} = request(delete, "/gateway/stomp"),
    {200, StompGw2} = request(get, "/gateway/stomp"),
    assert_gw_unloaded(StompGw2),
    ok.

t_gateway_stomp(_) ->
    {200, Gw} = request(get, "/gateway/stomp"),
    assert_gw_unloaded(Gw),
    %% post
    GwConf = #{
        name => <<"stomp">>,
        frame => #{
            max_headers => 5,
            max_headers_length => 100,
            max_body_length => 100
        },
        listeners => [
            #{name => <<"def">>, type => <<"tcp">>, bind => <<"61613">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/stomp"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{frame => #{max_headers => 10}}),
    {200, _} = request(put, "/gateway/stomp", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/stomp"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/stomp").

t_gateway_mqttsn(_) ->
    {200, Gw} = request(get, "/gateway/mqttsn"),
    assert_gw_unloaded(Gw),
    %% post
    GwConf = #{
        name => <<"mqttsn">>,
        gateway_id => 1,
        broadcast => true,
        predefined => [#{id => 1, topic => <<"t/a">>}],
        enable_qos3 => true,
        listeners => [
            #{name => <<"def">>, type => <<"udp">>, bind => <<"1884">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/mqttsn"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{predefined => []}),
    {200, _} = request(put, "/gateway/mqttsn", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/mqttsn"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/mqttsn").

t_gateway_coap(_) ->
    {200, Gw} = request(get, "/gateway/coap"),
    assert_gw_unloaded(Gw),
    %% post
    GwConf = #{
        name => <<"coap">>,
        heartbeat => <<"60s">>,
        connection_required => true,
        listeners => [
            #{name => <<"def">>, type => <<"udp">>, bind => <<"5683">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/coap"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{heartbeat => <<"10s">>}),
    {200, _} = request(put, "/gateway/coap", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/coap"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/coap").

t_gateway_lwm2m(_) ->
    {200, Gw} = request(get, "/gateway/lwm2m"),
    assert_gw_unloaded(Gw),
    %% post
    GwConf = #{
        name => <<"lwm2m">>,
        xml_dir => <<"../../lib/emqx_gateway/src/lwm2m/lwm2m_xml">>,
        lifetime_min => <<"1s">>,
        lifetime_max => <<"1000s">>,
        qmode_time_window => <<"30s">>,
        auto_observe => true,
        translators => #{
            command => #{topic => <<"dn/#">>},
            response => #{topic => <<"up/resp">>},
            notify => #{topic => <<"up/resp">>},
            register => #{topic => <<"up/resp">>},
            update => #{topic => <<"up/resp">>}
        },
        listeners => [
            #{name => <<"def">>, type => <<"udp">>, bind => <<"5783">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/lwm2m"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{qmode_time_window => <<"10s">>}),
    {200, _} = request(put, "/gateway/lwm2m", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/lwm2m"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/lwm2m").

t_gateway_exproto(_) ->
    {200, Gw} = request(get, "/gateway/exproto"),
    assert_gw_unloaded(Gw),
    %% post
    GwConf = #{
        name => <<"exproto">>,
        server => #{bind => <<"9100">>},
        handler => #{address => <<"http://127.0.0.1:9001">>},
        listeners => [
            #{name => <<"def">>, type => <<"tcp">>, bind => <<"7993">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/exproto"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{server => #{bind => <<"9200">>}}),
    {200, _} = request(put, "/gateway/exproto", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/exproto"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/exproto").

t_gateway_exproto_with_ssl(_) ->
    {200, Gw} = request(get, "/gateway/exproto"),
    assert_gw_unloaded(Gw),

    SslSvrOpts = ssl_server_opts(),
    SslCliOpts = ssl_client_opts(),
    %% post
    GwConf = #{
        name => <<"exproto">>,
        server => #{
            bind => <<"9100">>,
            ssl_options => SslSvrOpts
        },
        handler => #{
            address => <<"http://127.0.0.1:9001">>,
            ssl_options => SslCliOpts#{enable => true}
        },
        listeners => [
            #{name => <<"def">>, type => <<"tcp">>, bind => <<"7993">>}
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/exproto"),
    assert_confs(GwConf, ConfResp),
    %% put
    GwConf2 = emqx_map_lib:deep_merge(GwConf, #{
        server => #{
            bind => <<"9200">>,
            ssl_options => SslCliOpts
        }
    }),
    {200, _} = request(put, "/gateway/exproto", maps:without([name, listeners], GwConf2)),
    {200, ConfResp2} = request(get, "/gateway/exproto"),
    assert_confs(GwConf2, ConfResp2),
    {204, _} = request(delete, "/gateway/exproto").

t_authn(_) ->
    GwConf = #{name => <<"stomp">>},
    {201, _} = request(post, "/gateway", GwConf),
    {204, _} = request(get, "/gateway/stomp/authentication"),

    AuthConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"clientid">>
    },
    {201, _} = request(post, "/gateway/stomp/authentication", AuthConf),
    {200, ConfResp} = request(get, "/gateway/stomp/authentication"),
    assert_confs(AuthConf, ConfResp),

    AuthConf2 = maps:merge(AuthConf, #{user_id_type => <<"username">>}),
    {200, _} = request(put, "/gateway/stomp/authentication", AuthConf2),

    {200, ConfResp2} = request(get, "/gateway/stomp/authentication"),
    assert_confs(AuthConf2, ConfResp2),

    {204, _} = request(delete, "/gateway/stomp/authentication"),
    {204, _} = request(get, "/gateway/stomp/authentication"),
    {204, _} = request(delete, "/gateway/stomp").

t_authn_data_mgmt(_) ->
    GwConf = #{name => <<"stomp">>},
    {201, _} = request(post, "/gateway", GwConf),
    {204, _} = request(get, "/gateway/stomp/authentication"),

    AuthConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"clientid">>
    },
    {201, _} = request(post, "/gateway/stomp/authentication", AuthConf),
    {200, ConfResp} = request(get, "/gateway/stomp/authentication"),
    assert_confs(AuthConf, ConfResp),

    User1 = #{
        user_id => <<"test">>,
        password => <<"123456">>,
        is_superuser => false
    },
    {201, _} = request(post, "/gateway/stomp/authentication/users", User1),
    {200, #{data := [UserRespd1]}} = request(get, "/gateway/stomp/authentication/users"),
    assert_confs(UserRespd1, User1),

    {200, UserRespd2} = request(
        get,
        "/gateway/stomp/authentication/users/test"
    ),
    assert_confs(UserRespd2, User1),

    {200, UserRespd3} = request(
        put,
        "/gateway/stomp/authentication/users/test",
        #{
            password => <<"654321">>,
            is_superuser => true
        }
    ),
    assert_confs(UserRespd3, User1#{is_superuser => true}),

    {200, UserRespd4} = request(
        get,
        "/gateway/stomp/authentication/users/test"
    ),
    assert_confs(UserRespd4, User1#{is_superuser => true}),

    {204, _} = request(delete, "/gateway/stomp/authentication/users/test"),

    {200, #{data := []}} = request(
        get,
        "/gateway/stomp/authentication/users"
    ),

    ImportUri = emqx_dashboard_api_test_helpers:uri(
        ["gateway", "stomp", "authentication", "import_users"]
    ),

    Dir = code:lib_dir(emqx_authn, test),
    JSONFileName = filename:join([Dir, <<"data/user-credentials.json">>]),
    {ok, JSONData} = file:read_file(JSONFileName),
    {ok, 204, _} = emqx_dashboard_api_test_helpers:multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.json", JSONData}
    ]),

    CSVFileName = filename:join([Dir, <<"data/user-credentials.csv">>]),
    {ok, CSVData} = file:read_file(CSVFileName),
    {ok, 204, _} = emqx_dashboard_api_test_helpers:multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.csv", CSVData}
    ]),

    {204, _} = request(delete, "/gateway/stomp/authentication"),
    {204, _} = request(get, "/gateway/stomp/authentication"),
    {204, _} = request(delete, "/gateway/stomp").

t_listeners_tcp(_) ->
    GwConf = #{name => <<"stomp">>},
    {201, _} = request(post, "/gateway", GwConf),
    {404, _} = request(get, "/gateway/stomp/listeners"),
    LisConf = #{
        name => <<"def">>,
        type => <<"tcp">>,
        bind => <<"61613">>
    },
    {201, _} = request(post, "/gateway/stomp/listeners", LisConf),
    {200, ConfResp} = request(get, "/gateway/stomp/listeners"),
    assert_confs([LisConf], ConfResp),
    {200, ConfResp1} = request(get, "/gateway/stomp/listeners/stomp:tcp:def"),
    assert_confs(LisConf, ConfResp1),

    LisConf2 = maps:merge(LisConf, #{bind => <<"61614">>}),
    {200, _} = request(
        put,
        "/gateway/stomp/listeners/stomp:tcp:def",
        LisConf2
    ),

    {200, ConfResp2} = request(get, "/gateway/stomp/listeners/stomp:tcp:def"),
    assert_confs(LisConf2, ConfResp2),

    {204, _} = request(delete, "/gateway/stomp/listeners/stomp:tcp:def"),
    {404, _} = request(get, "/gateway/stomp/listeners/stomp:tcp:def"),
    {204, _} = request(delete, "/gateway/stomp").

t_listeners_authn(_) ->
    GwConf = #{
        name => <<"stomp">>,
        listeners => [
            #{
                name => <<"def">>,
                type => <<"tcp">>,
                bind => <<"61613">>
            }
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/stomp"),
    assert_confs(GwConf, ConfResp),

    AuthConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"clientid">>
    },
    Path = "/gateway/stomp/listeners/stomp:tcp:def/authentication",
    {201, _} = request(post, Path, AuthConf),
    {200, ConfResp2} = request(get, Path),
    assert_confs(AuthConf, ConfResp2),

    AuthConf2 = maps:merge(AuthConf, #{user_id_type => <<"username">>}),
    {200, _} = request(put, Path, AuthConf2),

    {200, ConfResp3} = request(get, Path),
    assert_confs(AuthConf2, ConfResp3),

    {204, _} = request(delete, Path),
    %% FIXME: 204?
    {204, _} = request(get, Path),
    {204, _} = request(delete, "/gateway/stomp").

t_listeners_authn_data_mgmt(_) ->
    GwConf = #{
        name => <<"stomp">>,
        listeners => [
            #{
                name => <<"def">>,
                type => <<"tcp">>,
                bind => <<"61613">>
            }
        ]
    },
    {201, _} = request(post, "/gateway", GwConf),
    {200, ConfResp} = request(get, "/gateway/stomp"),
    assert_confs(GwConf, ConfResp),

    AuthConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"clientid">>
    },
    Path = "/gateway/stomp/listeners/stomp:tcp:def/authentication",
    {201, _} = request(post, Path, AuthConf),
    {200, ConfResp2} = request(get, Path),
    assert_confs(AuthConf, ConfResp2),

    User1 = #{
        user_id => <<"test">>,
        password => <<"123456">>,
        is_superuser => false
    },
    {201, _} = request(
        post,
        "/gateway/stomp/listeners/stomp:tcp:def/authentication/users",
        User1
    ),

    {200, #{data := [UserRespd1]}} = request(
        get,
        Path ++ "/users"
    ),
    assert_confs(UserRespd1, User1),

    {200, UserRespd2} = request(
        get,
        Path ++ "/users/test"
    ),
    assert_confs(UserRespd2, User1),

    {200, UserRespd3} = request(
        put,
        Path ++ "/users/test",
        #{password => <<"654321">>, is_superuser => true}
    ),
    assert_confs(UserRespd3, User1#{is_superuser => true}),

    {200, UserRespd4} = request(
        get,
        Path ++ "/users/test"
    ),
    assert_confs(UserRespd4, User1#{is_superuser => true}),

    {204, _} = request(
        delete,
        Path ++ "/users/test"
    ),

    {200, #{data := []}} = request(
        get,
        Path ++ "/users"
    ),

    ImportUri = emqx_dashboard_api_test_helpers:uri(
        ["gateway", "stomp", "listeners", "stomp:tcp:def", "authentication", "import_users"]
    ),

    Dir = code:lib_dir(emqx_authn, test),
    JSONFileName = filename:join([Dir, <<"data/user-credentials.json">>]),
    {ok, JSONData} = file:read_file(JSONFileName),
    {ok, 204, _} = emqx_dashboard_api_test_helpers:multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.json", JSONData}
    ]),

    CSVFileName = filename:join([Dir, <<"data/user-credentials.csv">>]),
    {ok, CSVData} = file:read_file(CSVFileName),
    {ok, 204, _} = emqx_dashboard_api_test_helpers:multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.csv", CSVData}
    ]),

    {204, _} = request(delete, "/gateway/stomp").

t_authn_fuzzy_search(_) ->
    GwConf = #{name => <<"stomp">>},
    {201, _} = request(post, "/gateway", GwConf),
    {204, _} = request(get, "/gateway/stomp/authentication"),

    AuthConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"clientid">>
    },
    {201, _} = request(post, "/gateway/stomp/authentication", AuthConf),
    {200, ConfResp} = request(get, "/gateway/stomp/authentication"),
    assert_confs(AuthConf, ConfResp),

    Checker = fun({User, Fuzzy}) ->
        {200, #{data := [UserRespd]}} = request(
            get, "/gateway/stomp/authentication/users", Fuzzy
        ),
        assert_confs(UserRespd, User)
    end,

    Create = fun(User) ->
        {201, _} = request(post, "/gateway/stomp/authentication/users", User)
    end,

    UserDatas = [
        #{
            user_id => <<"test">>,
            password => <<"123456">>,
            is_superuser => false
        },
        #{
            user_id => <<"foo">>,
            password => <<"123456">>,
            is_superuser => true
        }
    ],

    FuzzyDatas = [[{<<"like_user_id">>, <<"test">>}], [{<<"is_superuser">>, <<"true">>}]],

    lists:foreach(Create, UserDatas),
    lists:foreach(Checker, lists:zip(UserDatas, FuzzyDatas)),

    {204, _} = request(delete, "/gateway/stomp/authentication"),
    {204, _} = request(get, "/gateway/stomp/authentication"),
    {204, _} = request(delete, "/gateway/stomp").

%%--------------------------------------------------------------------
%% Asserts

assert_gw_unloaded(Gateway) ->
    ?assertEqual(<<"unloaded">>, maps:get(status, Gateway)).

assert_bad_request(BadReq) ->
    ?assertEqual(<<"BAD_REQUEST">>, maps:get(code, BadReq)).
