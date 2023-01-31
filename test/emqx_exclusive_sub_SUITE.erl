%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exclusive_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TAB, emqx_exclusive_subscription).
-define(EXCLUSIVE_TOPIC, <<"$exclusive/t/1">>).
-define(NORMAL_TOPIC, <<"t/1">>).

-define(CHECK_SUB(Client, Code), ?CHECK_SUB(Client, ?EXCLUSIVE_TOPIC, Code)).
-define(CHECK_SUB(Client, Topic, Code),
    {ok, _, [Code]} = emqtt:subscribe(Client, Topic, [])
).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    enable_exclusive_sub(true),
    Config.

end_per_suite(_Config) ->
    reset_zone_env(),
    emqx_ct_helpers:stop_apps([]).

end_per_testcase(_TestCase, _Config) ->
    mnesia:clear_table(?TAB).

%% test that this feature is working during the whole session life cycle
t_exclusive_sub(_) ->
    {ok, C1} = emqtt:start_link([
        {clientid, <<"client1">>},
        {clean_start, false},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 100}}
    ]),
    {ok, _} = emqtt:connect(C1),
    ?CHECK_SUB(C1, 0),

    {ok, C2} = emqtt:start_link([
        {clientid, <<"client2">>},
        {clean_start, false},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C2),
    ?CHECK_SUB(C2, ?RC_QUOTA_EXCEEDED),

    %% keep exclusive even disconnected
    ok = emqtt:disconnect(C1),
    timer:sleep(1000),

    ?CHECK_SUB(C2, ?RC_QUOTA_EXCEEDED),

    ok = emqtt:disconnect(C2).

%% test this feature does not interfere with normal subs
t_allow_normal_sub(_) ->
    {ok, C1} = emqtt:start_link([
        {clientid, <<"client1">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C1),
    ?CHECK_SUB(C1, 0),

    {ok, C2} = emqtt:start_link([
        {clientid, <<"client2">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C2),
    ?CHECK_SUB(C2, ?NORMAL_TOPIC, 0),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

%% test the exclusive topics can be released correctly
t_unsub(_) ->
    {ok, C1} = emqtt:start_link([
        {clientid, <<"client1">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C1),
    ?CHECK_SUB(C1, 0),

    {ok, C2} = emqtt:start_link([
        {clientid, <<"client2">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C2),
    ?CHECK_SUB(C2, ?RC_QUOTA_EXCEEDED),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, ?EXCLUSIVE_TOPIC),

    ?CHECK_SUB(C2, 0),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

%% test whether the exclusive topics would auto-clean after the session was cleaned
t_clean_session(_) ->
    erlang:process_flag(trap_exit, true),
    {ok, C1} = emqtt:start_link([
        {clientid, <<"client1">>},
        {clean_start, true},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 0}}
    ]),
    {ok, _} = emqtt:connect(C1),
    ?CHECK_SUB(C1, 0),

    {ok, C2} = emqtt:start_link([
        {clientid, <<"client2">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C2),
    ?CHECK_SUB(C2, ?RC_QUOTA_EXCEEDED),

    %% auto clean when session was cleand
    ok = emqtt:disconnect(C1),

    timer:sleep(1000),

    ?CHECK_SUB(C2, 0),

    ok = emqtt:disconnect(C2).

%% test the feature switch
t_feat_disabled(_) ->
    enable_exclusive_sub(false),

    {ok, C1} = emqtt:start_link([
        {clientid, <<"client1">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C1),
    ?CHECK_SUB(C1, ?RC_TOPIC_FILTER_INVALID),
    ok = emqtt:disconnect(C1),

    enable_exclusive_sub(true).

enable_exclusive_sub(Enable) ->
    emqx_zone:set_env(
        external,
        '$mqtt_sub_caps',
        #{exclusive_subscription => Enable}
    ),
    timer:sleep(50).

reset_zone_env() ->
    emqx_zone:unset_env(external, '$mqtt_sub_caps'),
    timer:sleep(50).
