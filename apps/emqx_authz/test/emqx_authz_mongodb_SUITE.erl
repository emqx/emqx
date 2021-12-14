%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(MONGO_HOST, "mongo").
-define(MONGO_PORT, 27017).
-define(MONGO_CLIENT, 'emqx_authz_mongo_SUITE_client').

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_TestCase, Config) ->
    {ok, _} = mc_worker_api:connect(mongo_config()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = reset_samples(),
    ok = mc_worker_api:disconnect(?MONGO_CLIENT).

init_per_suite(Config) ->
    case emqx_authz_test_lib:is_tcp_server_available(?MONGO_HOST, ?MONGO_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                   [emqx_conf, emqx_authz],
                   fun set_special_configs/1
                  ),
            ok = start_apps([emqx_resource, emqx_connector]),
            Config;
        false ->
            {skip, no_mongo}
    end.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();

set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------


t_topic_rules(_Config) ->
    ClientInfo = #{clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    %% No rules

    ok = setup_samples([]),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{deny, subscribe, <<"#">>},
            {deny, subscribe, <<"subs">>},
            {deny, publish, <<"pub">>}]),

    %% Publish rules

    Samples0 = populate_records(
                 [#{<<"topics">> => [<<"eq testpub1/${username}">>]},
                  #{<<"topics">> => [<<"testpub2/${clientid}">>, <<"testpub3/#">>]}],
                 #{<<"permission">> => <<"allow">>,
                   <<"action">> => <<"publish">>,
                   <<"username">> => <<"username">>}),

    ok = setup_samples(Samples0),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{deny, publish, <<"testpub1/username">>},
            {allow, publish, <<"testpub1/${username}">>},
            {allow, publish, <<"testpub2/clientid">>},
            {allow, publish, <<"testpub3/foobar">>},

            {deny, publish, <<"testpub2/username">>},
            {deny, publish, <<"testpub1/clientid">>},


            {deny, subscribe, <<"testpub1/username">>},
            {deny, subscribe, <<"testpub2/clientid">>},
            {deny, subscribe, <<"testpub3/foobar">>}]),

    %% Subscribe rules

    Samples1 = populate_records(
                 [#{<<"topics">> => [<<"eq testsub1/${username}">>]},
                  #{<<"topics">> => [<<"testsub2/${clientid}">>, <<"testsub3/#">>]}],
                 #{<<"permission">> => <<"allow">>,
                   <<"action">> => <<"subscribe">>,
                   <<"username">> => <<"username">>}),

    ok = setup_samples(Samples1),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{deny, subscribe, <<"testsub1/username">>},
            {allow, subscribe, <<"testsub1/${username}">>},
            {allow, subscribe, <<"testsub2/clientid">>},
            {allow, subscribe, <<"testsub3/foobar">>},
            {allow, subscribe, <<"testsub3/+/foobar">>},
            {allow, subscribe, <<"testsub3/#">>},

            {deny, subscribe, <<"testsub2/username">>},
            {deny, subscribe, <<"testsub1/clientid">>},
            {deny, subscribe, <<"testsub4/foobar">>},
            {deny, publish, <<"testsub1/username">>},
            {deny, publish, <<"testsub2/clientid">>},
            {deny, publish, <<"testsub3/foobar">>}]),

    %% All rules

    Samples2 = populate_records(
                 [#{<<"topics">> => [<<"eq testall1/${username}">>]},
                  #{<<"topics">> => [<<"testall2/${clientid}">>, <<"testall3/#">>]}],
                 #{<<"permission">> => <<"allow">>,
                   <<"action">> => <<"all">>,
                   <<"username">> => <<"username">>}),

    ok = setup_samples(Samples2),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{deny, subscribe, <<"testall1/username">>},
            {allow, subscribe, <<"testall1/${username}">>},
            {allow, subscribe, <<"testall2/clientid">>},
            {allow, subscribe, <<"testall3/foobar">>},
            {allow, subscribe, <<"testall3/+/foobar">>},
            {allow, subscribe, <<"testall3/#">>},
            {deny, publish, <<"testall1/username">>},
            {allow, publish, <<"testall1/${username}">>},
            {allow, publish, <<"testall2/clientid">>},
            {allow, publish, <<"testall3/foobar">>},

            {deny, subscribe, <<"testall2/username">>},
            {deny, subscribe, <<"testall1/clientid">>},
            {deny, subscribe, <<"testall4/foobar">>},
            {deny, publish, <<"testall2/username">>},
            {deny, publish, <<"testall1/clientid">>},
            {deny, publish, <<"testall4/foobar">>}]).


t_complex_selector(_) ->
    ClientInfo = #{clientid => clientid,
                   username => "username",
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    Samples = [#{<<"x">> => #{<<"u">> => <<"username">>,
                              <<"c">> => [#{<<"c">> => <<"clientid">>}],
                              <<"y">> => 1},
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"publish">>,
                 <<"topics">> => [<<"t">>]
                }],

    ok = setup_samples(Samples),
    ok = setup_config(
           #{<<"selector">> => #{<<"x">> => #{<<"u">> => <<"${username}">>,
                                              <<"c">> => [#{<<"c">> => <<"${clientid}">>}],
                                              <<"y">> => 1}
                                }
            }),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, publish, <<"t">>}]).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

populate_records(AclRecords, AdditionalData) ->
    [maps:merge(Record, AdditionalData) || Record <- AclRecords].

setup_samples(AclRecords) ->
    ok = reset_samples(),
    {{true, _}, _} = mc_worker_api:insert(?MONGO_CLIENT, <<"acl">>, AclRecords),
    ok.

reset_samples() ->
    {true, _} = mc_worker_api:delete(?MONGO_CLIENT, <<"acl">>, #{}),
    ok.

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
      raw_mongo_authz_config(),
      SpecialParams).

raw_mongo_authz_config() ->
    #{
        <<"type">> => <<"mongodb">>,
        <<"enable">> => <<"true">>,

        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"acl">>,
        <<"server">> => mongo_server(),

        <<"selector">> => #{<<"username">> => <<"${username}">>}
    }.

mongo_server() ->
    iolist_to_binary(
      io_lib:format(
        "~s:~b",
        [?MONGO_HOST, ?MONGO_PORT])).

mongo_config() ->
    [
     {database, <<"mqtt">>},
     {host, ?MONGO_HOST},
     {port, ?MONGO_PORT},
     {register, ?MONGO_CLIENT}
    ].

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
