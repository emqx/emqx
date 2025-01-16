%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_access_control:set_default_authn_restrictive(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_access_control:set_default_authn_permissive(),
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok = emqx_hooks:del('client.authorize', {?MODULE, authz_stub}),
    ok = emqx_hooks:del('client.authenticate', {?MODULE, quick_deny_anonymous_authn}).

t_authenticate(_) ->
    ClientInfo = clientinfo(),
    ?assertMatch({error, not_authorized}, emqx_access_control:authenticate(ClientInfo)),
    ?assertMatch(
        {error, not_authorized}, emqx_access_control:authenticate(ClientInfo#{enable_authn => true})
    ),
    ?assertMatch(
        {error, not_authorized},
        emqx_access_control:authenticate(ClientInfo#{enable_authn => quick_deny_anonymous})
    ),
    ?assertMatch({ok, _}, emqx_access_control:authenticate(ClientInfo#{enable_authn => false})).

t_authorize(_) ->
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), ?AUTHZ_PUBLISH, <<"t">>)).

t_delayed_authorize(_) ->
    RawTopic = <<"$delayed/1/foo/2">>,
    InvalidTopic = <<"$delayed/1/foo/3">>,
    Topic = <<"foo/2">>,

    ok = emqx_hooks:put('client.authorize', {?MODULE, authz_stub, [Topic]}, ?HP_AUTHZ),

    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), ?AUTHZ_PUBLISH, RawTopic)),

    ?assertEqual(
        deny, emqx_access_control:authorize(clientinfo(), ?AUTHZ_PUBLISH, InvalidTopic)
    ),
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

authz_stub(_Client, _Action, ValidTopic, _DefaultResult, ValidTopic) -> {stop, #{result => allow}};
authz_stub(_Client, _Action, _Topic, _DefaultResult, _ValidTopic) -> {stop, #{result => deny}}.

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
            mountpoint => undefined
        },
        InitProps
    ).
