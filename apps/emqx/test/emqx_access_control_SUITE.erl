%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules([router, broker]),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

end_per_testcase(t_delayed_authorize, Config) ->
    meck:unload(emqx_access_control),
    Config;
end_per_testcase(_, Config) ->
    Config.

t_authenticate(_) ->
    ?assertMatch({ok, _}, emqx_access_control:authenticate(clientinfo())).

t_authorize(_) ->
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"t">>, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish, <<"t">>)).

t_delayed_authorize(_) ->
    RawTopic = "$dealyed/1/foo/2",
    InvalidTopic = "$dealyed/1/foo/3",
    Topic = "foo/2",

    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_access_control,
        do_authorize,
        fun
            (_, _, Topic) -> allow;
            (_, _, _) -> deny
        end
    ),

    Publish1 = ?PUBLISH_PACKET(?QOS_0, RawTopic, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish1, RawTopic)),

    Publish2 = ?PUBLISH_PACKET(?QOS_0, InvalidTopic, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish2, InvalidTopic)),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

clientinfo() -> clientinfo(#{}).
clientinfo(InitProps) ->
    maps:merge(
        #{
            zone => default,
            listener => {tcp, default},
            protocol => mqtt,
            peerhost => {127, 0, 0, 1},
            clientid => <<"clientid">>,
            username => <<"username">>,
            password => <<"passwd">>,
            is_superuser => false,
            peercert => undefined,
            mountpoint => undefined
        },
        InitProps
    ).

toggle_auth(Bool) when is_boolean(Bool) ->
    emqx_config:put_zone_conf(default, [auth, enable], Bool).
