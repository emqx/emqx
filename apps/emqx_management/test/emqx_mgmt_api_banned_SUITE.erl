%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_banned_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_create(_Config) ->
    Now = erlang:system_time(second),
    At = emqx_banned:to_rfc3339(Now),
    Until = emqx_banned:to_rfc3339(Now + 3),
    ClientId = <<"TestClient测试"/utf8>>,
    By = <<"banned suite测试组"/utf8>>,
    Reason = <<"test测试"/utf8>>,
    As = <<"clientid">>,
    ClientIdBanned = #{
        as => As,
        who => ClientId,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, ClientIdBannedRes} = create_banned(ClientIdBanned),
    ?assertEqual(
        #{
            <<"as">> => As,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => Until,
            <<"who">> => ClientId
        },
        ClientIdBannedRes
    ),
    PeerHost = <<"192.168.2.13">>,
    PeerHostBanned = #{
        as => <<"peerhost">>,
        who => PeerHost,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, PeerHostBannedRes} = create_banned(PeerHostBanned),
    ?assertEqual(
        #{
            <<"as">> => <<"peerhost">>,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => Until,
            <<"who">> => PeerHost
        },
        PeerHostBannedRes
    ),
    {ok, #{<<"data">> := List}} = list_banned(),
    Bans = lists:sort(lists:map(fun(#{<<"who">> := W, <<"as">> := A}) -> {A, W} end, List)),
    ?assertEqual([{<<"clientid">>, ClientId}, {<<"peerhost">>, PeerHost}], Bans),
    ok.

t_create_failed(_Config) ->
    Now = erlang:system_time(second),
    At = emqx_banned:to_rfc3339(Now),
    Until = emqx_banned:to_rfc3339(Now + 10),
    Who = <<"BadHost"/utf8>>,
    By = <<"banned suite测试组"/utf8>>,
    Reason = <<"test测试"/utf8>>,
    As = <<"peerhost">>,
    BadPeerHost = #{
        as => As,
        who => Who,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    BadRequest = {error, {"HTTP/1.1", 400, "Bad Request"}},
    ?assertEqual(BadRequest, create_banned(BadPeerHost)),
    Expired = BadPeerHost#{
        until => emqx_banned:to_rfc3339(Now - 1),
        who => <<"127.0.0.1">>
    },
    ?assertEqual(BadRequest, create_banned(Expired)),
    ok.

t_delete(_Config) ->
    Now = erlang:system_time(second),
    At = emqx_banned:to_rfc3339(Now),
    Until = emqx_banned:to_rfc3339(Now + 3),
    Who = <<"TestClient-"/utf8>>,
    By = <<"banned suite 中"/utf8>>,
    Reason = <<"test测试"/utf8>>,
    As = <<"clientid">>,
    Banned = #{
        as => clientid,
        who => Who,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, _} = create_banned(Banned),
    ?assertMatch({ok, _}, delete_banned(binary_to_list(As), binary_to_list(Who))),
    ?assertMatch(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        delete_banned(binary_to_list(As), binary_to_list(Who))
    ),
    ok.

list_banned() ->
    Path = emqx_mgmt_api_test_util:api_path(["banned"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Apps} -> {ok, emqx_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

create_banned(Banned) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["banned"]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Banned) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

delete_banned(As, Who) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["banned", As, Who]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).
