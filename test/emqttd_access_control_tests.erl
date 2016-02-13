%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_access_control_tests).

-ifdef(TEST).

-include("emqttd.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(AC, emqttd_access_control).

acl_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [?_test(t_reload_acl()),
      ?_test(t_register_mod()),
      ?_test(t_unregister_mod()),
      ?_test(t_check_acl())
     ]}.

setup() ->
    AclOpts = [
        {auth, [{anonymous, []}]},
        {acl, [ %% ACL config
            %% Internal ACL module
            {internal,  [{file, "./testdata/test_acl.config"}, {nomatch, allow}]}
        ]}
    ],
    ?AC:start_link(AclOpts).

teardown({ok, _Pid}) ->
    ?AC:stop().

t_reload_acl() ->
    ?assertEqual([ok], ?AC:reload_acl()).

t_register_mod() ->
    ?AC:register_mod(acl, emqttd_acl_test_mod, []),
    ?assertMatch([{emqttd_acl_test_mod, _, 0},
                  {emqttd_acl_internal, _, 0}],
                 ?AC:lookup_mods(acl)),
    ?AC:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
    ?AC:register_mod(auth, emqttd_auth_dashboard, [], 99),
    ?assertMatch([{emqttd_auth_dashboard, _, 99},
                  {emqttd_auth_anonymous_test_mod, _, 0},
                  {emqttd_auth_anonymous, _, 0}],
                 ?AC:lookup_mods(auth)).

t_unregister_mod() ->
    ?AC:register_mod(acl, emqttd_acl_test_mod, []),
    ?assertMatch([{emqttd_acl_test_mod, _, 0}, {emqttd_acl_internal, _, 0}],
                 ?AC:lookup_mods(acl)),
    ?AC:unregister_mod(acl, emqttd_acl_test_mod),
    timer:sleep(5),
    ?assertMatch([{emqttd_acl_internal, _, 0}], ?AC:lookup_mods(acl)),

    ?AC:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
    ?assertMatch([{emqttd_auth_anonymous_test_mod, _, 0},
                  {emqttd_auth_anonymous, _, 0}],
                 ?AC:lookup_mods(auth)),

    ?AC:unregister_mod(auth, emqttd_auth_anonymous_test_mod),
    timer:sleep(5),
    ?assertMatch([{emqttd_auth_anonymous, _, 0}], ?AC:lookup_mods(auth)).

t_check_acl() ->
    User1 = #mqtt_client{client_id = <<"client1">>, username = <<"testuser">>},
    User2 = #mqtt_client{client_id = <<"client2">>, username = <<"xyz">>},
    ?assertEqual(allow, ?AC:check_acl(User1, subscribe, <<"users/testuser/1">>)),
    ?assertEqual(allow, ?AC:check_acl(User1, subscribe, <<"clients/client1">>)),
    ?assertEqual(deny, ?AC:check_acl(User1, subscribe, <<"clients/client1/x/y">>)),
    ?assertEqual(allow, ?AC:check_acl(User1, publish, <<"users/testuser/1">>)),
    ?assertEqual(allow, ?AC:check_acl(User1, subscribe, <<"a/b/c">>)),
    ?assertEqual(deny, ?AC:check_acl(User2, subscribe, <<"a/b/c">>)).

-endif.

