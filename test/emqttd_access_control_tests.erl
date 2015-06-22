%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd_access_control tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_control_tests).

-include("emqttd.hrl").

-ifdef(TEST).

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
            ?assertMatch([{emqttd_acl_test_mod, _}, {emqttd_acl_internal, _}],
                          emqttd_access_control:lookup_mods(acl)),
	    emqttd_access_control:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
	    ?assertMatch([{emqttd_auth_anonymous_test_mod, _}, {emqttd_auth_anonymous, _}],
                          emqttd_access_control:lookup_mods(auth))
        end).

unregister_mod_test() ->
    with_acl(
        fun() ->
            emqttd_access_control:register_mod(acl,emqttd_acl_test_mod, []),
            ?assertMatch([{emqttd_acl_test_mod, _}, {emqttd_acl_internal, _}],
                          emqttd_access_control:lookup_mods(acl)),
            emqttd_access_control:unregister_mod(acl, emqttd_acl_test_mod),
            timer:sleep(5),
            ?assertMatch([{emqttd_acl_internal, _}], emqttd_access_control:lookup_mods(acl)),
	
	    emqttd_access_control:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
	    ?assertMatch([{emqttd_auth_anonymous_test_mod, _}, {emqttd_auth_anonymous, _}],
                          emqttd_access_control:lookup_mods(auth)),
		
	    emqttd_access_control:unregister_mod(auth, emqttd_auth_anonymous_test_mod),
            timer:sleep(5),
            ?assertMatch([{emqttd_auth_anonymous, _}], emqttd_access_control:lookup_mods(auth))
        end).

check_acl_test() ->
    with_acl(
        fun() ->
            User1 = #mqtt_client{clientid = <<"client1">>, username = <<"testuser">>},
            User2 = #mqtt_client{clientid = <<"client2">>, username = <<"xyz">>},
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
    application:set_env(emqttd, access, AclOpts),
    emqttd_access_control:start_link(),
    Fun(),
    emqttd_access_control:stop().

-endif.

