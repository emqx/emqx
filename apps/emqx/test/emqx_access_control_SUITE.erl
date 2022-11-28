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
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules([router, broker]),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok = emqx_hooks:del('client.authorize', {?MODULE, authz_stub}),
    ok = emqx_hooks:del('client.authenticate', {?MODULE, quick_deny_anonymous_authn}).

t_authenticate(_) ->
    ?assertMatch({ok, _}, emqx_access_control:authenticate(clientinfo())).

t_authorize(_) ->
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"t">>, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish, <<"t">>)).

t_delayed_authorize(_) ->
    RawTopic = <<"$delayed/1/foo/2">>,
    InvalidTopic = <<"$delayed/1/foo/3">>,
    Topic = <<"foo/2">>,

    ok = emqx_hooks:put('client.authorize', {?MODULE, authz_stub, [Topic]}, ?HP_AUTHZ),

    Publish1 = ?PUBLISH_PACKET(?QOS_0, RawTopic, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish1, RawTopic)),

    Publish2 = ?PUBLISH_PACKET(?QOS_0, InvalidTopic, 1, <<"payload">>),
    ?assertEqual(deny, emqx_access_control:authorize(clientinfo(), Publish2, InvalidTopic)),
    ok.

t_quick_deny_anonymous(_) ->
    ok = emqx_hooks:put(
        'client.authenticate',
        {?MODULE, quick_deny_anonymous_authn, []},
        ?HP_AUTHN
    ),

    RawClient0 = clientinfo(),
    RawClient = RawClient0#{username => undefined},

    %% No name, No authn
    Client1 = RawClient#{enable_authn => false},
    ?assertMatch({ok, _}, emqx_access_control:authenticate(Client1)),

    %% No name, With quick_deny_anonymous
    Client2 = RawClient#{enable_authn => quick_deny_anonymous},
    ?assertMatch({error, _}, emqx_access_control:authenticate(Client2)),

    %% Bad name, With quick_deny_anonymous
    Client3 = RawClient#{enable_authn => quick_deny_anonymous, username => <<"badname">>},
    ?assertMatch({error, _}, emqx_access_control:authenticate(Client3)),

    %% Good name, With quick_deny_anonymous
    Client4 = RawClient#{enable_authn => quick_deny_anonymous, username => <<"goodname">>},
    ?assertMatch({ok, _}, emqx_access_control:authenticate(Client4)),

    %% Name, With authn
    Client5 = RawClient#{enable_authn => true, username => <<"badname">>},
    ?assertMatch({error, _}, emqx_access_control:authenticate(Client5)),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

authz_stub(_Client, _PubSub, ValidTopic, _DefaultResult, ValidTopic) -> {stop, #{result => allow}};
authz_stub(_Client, _PubSub, _Topic, _DefaultResult, _ValidTopic) -> {stop, #{result => deny}}.

quick_deny_anonymous_authn(#{username := <<"badname">>}, _AuthResult) ->
    {stop, {error, not_authorized}};
quick_deny_anonymous_authn(_ClientInfo, _AuthResult) ->
    {stop, {ok, #{is_superuser => false}}}.

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
