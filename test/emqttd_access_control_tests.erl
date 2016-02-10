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

reload_acl_test() ->
    with_acl(
        fun() ->
            ?assertEqual([ok], emqttd_access_control:reload_acl())
        end).

register_mod_test() ->
    with_acl(
        fun() ->
            emqttd_access_control:register_mod(acl, emqttd_acl_test_mod, []),
            ?assertMatch([{emqttd_acl_test_mod, _, 0}, {emqttd_acl_internal, _, 0}],
                          emqttd_access_control:lookup_mods(acl)),
	    emqttd_access_control:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
	    emqttd_access_control:register_mod(auth, emqttd_auth_dashboard, [], 99),
	    ?assertMatch([{emqttd_auth_dashboard, _, 99},
                      {emqttd_auth_anonymous_test_mod, _, 0},
                      {emqttd_auth_anonymous, _, 0}],
                     emqttd_access_control:lookup_mods(auth))
        end).

unregister_mod_test() ->
    with_acl(
        fun() ->
            emqttd_access_control:register_mod(acl, emqttd_acl_test_mod, []),
            ?assertMatch([{emqttd_acl_test_mod, _, 0}, {emqttd_acl_internal, _, 0}],
                          emqttd_access_control:lookup_mods(acl)),
            emqttd_access_control:unregister_mod(acl, emqttd_acl_test_mod),
            timer:sleep(5),
            ?assertMatch([{emqttd_acl_internal, _, 0}], emqttd_access_control:lookup_mods(acl)),
	
	    emqttd_access_control:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
	    ?assertMatch([{emqttd_auth_anonymous_test_mod, _, 0}, {emqttd_auth_anonymous, _, 0}],
                          emqttd_access_control:lookup_mods(auth)),
		
	    emqttd_access_control:unregister_mod(auth, emqttd_auth_anonymous_test_mod),
            timer:sleep(5),
            ?assertMatch([{emqttd_auth_anonymous, _, 0}], emqttd_access_control:lookup_mods(auth))
        end).

check_acl_test() ->
    with_acl(
        fun() ->
            User1 = #mqtt_client{client_id = <<"client1">>, username = <<"testuser">>},
            User2 = #mqtt_client{client_id = <<"client2">>, username = <<"xyz">>},
            ?assertEqual(allow, emqttd_access_control:check_acl(User1, subscribe, <<"users/testuser/1">>)),
	    ?assertEqual(allow, emqttd_access_control:check_acl(User1, subscribe, <<"clients/client1">>)),
	    ?assertEqual(deny, emqttd_access_control:check_acl(User1, subscribe, <<"clients/client1/x/y">>)),
            ?assertEqual(allow, emqttd_access_control:check_acl(User1, publish, <<"users/testuser/1">>)),
            ?assertEqual(allow, emqttd_access_control:check_acl(User1, subscribe, <<"a/b/c">>)),
            ?assertEqual(deny, emqttd_access_control:check_acl(User2, subscribe, <<"a/b/c">>))
        end).

with_acl(Fun) ->
    process_flag(trap_exit, true),
    AclOpts = [
        {auth, [
            %% Authentication with username, password
            %{username, []},
            %% Authentication with clientid
            %{clientid, [{password, no}, {file, "etc/clients.config"}]},
            %% Allow all
            {anonymous, []}
        ]},
        %% ACL config
        {acl, [
            %% Internal ACL module
            {internal,  [{file, "../test/test_acl.config"}, {nomatch, allow}]}
        ]}
    ],
    %application:set_env(emqttd, access, AclOpts),
    emqttd_access_control:start_link(AclOpts),
    Fun(),
    emqttd_access_control:stop().

-endif.

