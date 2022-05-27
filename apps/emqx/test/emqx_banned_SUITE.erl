%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_banned_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema().

t_add_delete(_) ->
    Banned = #banned{
        who = {clientid, <<"TestClient">>},
        by = <<"banned suite">>,
        reason = <<"test">>,
        at = erlang:system_time(second),
        until = erlang:system_time(second) + 1
    },
    {ok, _} = emqx_banned:create(Banned),
    {error, {already_exist, Banned}} = emqx_banned:create(Banned),
    ?assertEqual(1, emqx_banned:info(size)),
    {error, {already_exist, Banned}} =
        emqx_banned:create(Banned#banned{until = erlang:system_time(second) + 100}),
    ?assertEqual(1, emqx_banned:info(size)),

    ok = emqx_banned:delete({clientid, <<"TestClient">>}),
    ?assertEqual(0, emqx_banned:info(size)).

t_check(_) ->
    {ok, _} = emqx_banned:create(#banned{who = {clientid, <<"BannedClient">>}}),
    {ok, _} = emqx_banned:create(#banned{who = {username, <<"BannedUser">>}}),
    {ok, _} = emqx_banned:create(#banned{who = {peerhost, {192, 168, 0, 1}}}),
    ?assertEqual(3, emqx_banned:info(size)),
    ClientInfo1 = #{
        clientid => <<"BannedClient">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfo2 = #{
        clientid => <<"client">>,
        username => <<"BannedUser">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfo3 = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {192, 168, 0, 1}
    },
    ClientInfo4 = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfo5 = #{},
    ClientInfo6 = #{clientid => <<"client1">>},
    ?assert(emqx_banned:check(ClientInfo1)),
    ?assert(emqx_banned:check(ClientInfo2)),
    ?assert(emqx_banned:check(ClientInfo3)),
    ?assertNot(emqx_banned:check(ClientInfo4)),
    ?assertNot(emqx_banned:check(ClientInfo5)),
    ?assertNot(emqx_banned:check(ClientInfo6)),
    ok = emqx_banned:delete({clientid, <<"BannedClient">>}),
    ok = emqx_banned:delete({username, <<"BannedUser">>}),
    ok = emqx_banned:delete({peerhost, {192, 168, 0, 1}}),
    ?assertNot(emqx_banned:check(ClientInfo1)),
    ?assertNot(emqx_banned:check(ClientInfo2)),
    ?assertNot(emqx_banned:check(ClientInfo3)),
    ?assertNot(emqx_banned:check(ClientInfo4)),
    ?assertEqual(0, emqx_banned:info(size)).

t_unused(_) ->
    catch emqx_banned:stop(),
    {ok, Banned} = emqx_banned:start_link(),
    {ok, _} = emqx_banned:create(#banned{
        who = {clientid, <<"BannedClient1">>},
        until = erlang:system_time(second)
    }),
    {ok, _} = emqx_banned:create(#banned{
        who = {clientid, <<"BannedClient2">>},
        until = erlang:system_time(second) - 1
    }),
    ?assertEqual(ignored, gen_server:call(Banned, unexpected_req)),
    ?assertEqual(ok, gen_server:cast(Banned, unexpected_msg)),
    ?assertEqual(ok, Banned ! ok),
    %% expiry timer
    timer:sleep(500),
    ok = emqx_banned:stop().
