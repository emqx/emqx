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
%%% emqttd_acl tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

all_modules_test() ->
    with_acl(
        fun() ->
            ?assertEqual([emqttd_acl_internal], emqttd_acl:all_modules())
        end).

reload_test() ->
    with_acl(
        fun() ->
            ?assertEqual([ok], emqttd_acl:reload())
        end).

register_mod_test() ->
    with_acl(
        fun() ->
            emqttd_acl:register_mod(acl_mysql),
            ?assertEqual([acl_mysql, emqttd_acl_internal], emqttd_acl:all_modules())
        end).

unregister_mod_test() ->
    with_acl(
        fun() ->
            emqttd_acl:register_mod(acl_mysql),
            ?assertEqual([acl_mysql, emqttd_acl_internal], emqttd_acl:all_modules()),
            emqttd_acl:unregister_mod(acl_mysql),
            timer:sleep(5),
            ?assertEqual([emqttd_acl_internal], emqttd_acl:all_modules())
        end).

check_test() ->
    with_acl(
        fun() ->
            User1 = #mqtt_user{clientid = <<"client1">>, username = <<"testuser">>},
            User2 = #mqtt_user{clientid = <<"client2">>, username = <<"xyz">>},
            ?assertEqual({ok, allow}, emqttd_acl:check(User1, subscribe, <<"users/testuser/1">>)),
            ?assertEqual({ok, allow}, emqttd_acl:check(User1, subscribe, <<"clients/client1">>)),
            ?assertEqual({ok, deny}, emqttd_acl:check(User1, subscribe, <<"clients/client1/x/y">>)),
            ?assertEqual({ok, allow}, emqttd_acl:check(User1, publish, <<"users/testuser/1">>)),
            ?assertEqual({ok, allow}, emqttd_acl:check(User1, subscribe, <<"a/b/c">>)),
            ?assertEqual({ok, deny}, emqttd_acl:check(User2, subscribe, <<"a/b/c">>))
        end).

with_acl(Fun) ->
    process_flag(trap_exit, true),
    AclOpts = [{file, "../test/test_acl.config"}],
    {ok, _AclSrv} = emqttd_acl:start_link(AclOpts),
    {ok, _InternalAcl} = emqttd_acl_internal:start_link(AclOpts),
    Fun(),
    emqttd_acl_internal:stop(),
    emqttd_acl:stop().

-endif.

