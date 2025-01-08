%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

t_create(_Config) ->
    Now = erlang:system_time(second),
    At = emqx_banned:to_rfc3339(Now),
    Until = emqx_banned:to_rfc3339(Now + 3),
    ClientId = <<"TestClient测试"/utf8>>,
    By = <<"banned suite测试组"/utf8>>,
    Reason = <<"test测试"/utf8>>,
    As = <<"clientid">>,

    %% ban by clientid
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

    %% ban by peerhost
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

    %% ban by username RE
    UsernameRE = <<"BannedUser.*">>,
    UsernameREBanned = #{
        as => <<"username_re">>,
        who => UsernameRE,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, UsernameREBannedRes} = create_banned(UsernameREBanned),
    ?assertEqual(
        #{
            <<"as">> => <<"username_re">>,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => Until,
            <<"who">> => UsernameRE
        },
        UsernameREBannedRes
    ),

    %% ban by clientid RE
    ClientIdRE = <<"BannedClient.*">>,
    ClientIdREBanned = #{
        as => <<"clientid_re">>,
        who => ClientIdRE,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, ClientIdREBannedRes} = create_banned(ClientIdREBanned),
    ?assertEqual(
        #{
            <<"as">> => <<"clientid_re">>,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => Until,
            <<"who">> => ClientIdRE
        },
        ClientIdREBannedRes
    ),

    %% ban by CIDR
    PeerHostNet = <<"192.168.0.0/24">>,
    PeerHostNetBanned = #{
        as => <<"peerhost_net">>,
        who => PeerHostNet,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    {ok, PeerHostNetBannedRes} = create_banned(PeerHostNetBanned),
    ?assertEqual(
        #{
            <<"as">> => <<"peerhost_net">>,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => Until,
            <<"who">> => PeerHostNet
        },
        PeerHostNetBannedRes
    ),

    {ok, #{<<"data">> := List}} = list_banned(),
    Bans = lists:sort(lists:map(fun(#{<<"who">> := W, <<"as">> := A}) -> {A, W} end, List)),
    ?assertEqual(
        [
            {<<"clientid">>, ClientId},
            {<<"clientid_re">>, ClientIdRE},
            {<<"peerhost">>, PeerHost},
            {<<"peerhost_net">>, PeerHostNet},
            {<<"username_re">>, UsernameRE}
        ],
        Bans
    ),

    ClientId2 = <<"TestClient2"/utf8>>,
    ClientIdBanned2 = #{
        as => As,
        who => ClientId2,
        by => By,
        reason => Reason,
        at => At
    },
    {ok, ClientIdBannedRes2} = create_banned(ClientIdBanned2),
    ?assertEqual(
        #{
            <<"as">> => As,
            <<"at">> => At,
            <<"by">> => By,
            <<"reason">> => Reason,
            <<"until">> => <<"infinity">>,
            <<"who">> => ClientId2
        },
        ClientIdBannedRes2
    ),
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

%% validate check_schema is true with bad content_type
t_create_with_bad_content_type(_Config) ->
    Now = erlang:system_time(second),
    At = emqx_banned:to_rfc3339(Now),
    Until = emqx_banned:to_rfc3339(Now + 3),
    Who = <<"TestClient-"/utf8>>,
    By = <<"banned suite 中"/utf8>>,
    Reason = <<"test测试"/utf8>>,
    Banned = #{
        as => clientid,
        who => Who,
        by => By,
        reason => Reason,
        at => At,
        until => Until
    },
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["banned"]),
    {error, {
        {"HTTP/1.1", 415, "Unsupported Media Type"},
        _Headers,
        MsgBin
    }} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            AuthHeader,
            Banned,
            #{'content-type' => "application/xml", return_all => true}
        ),
    ?assertEqual(
        #{
            <<"code">> => <<"UNSUPPORTED_MEDIA_TYPE">>,
            <<"message">> => <<"content-type:application/json Required">>
        },
        emqx_utils_json:decode(MsgBin)
    ).

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

t_clear(_Config) ->
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
    ?assertMatch({ok, _}, clear_banned()),
    ?assertMatch(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        delete_banned(binary_to_list(As), binary_to_list(Who))
    ),
    ok.

t_list_with_filters(_) ->
    setup_list_test_data(),

    %% clientid
    test_for_list("clientid=c1", [<<"c1">>]),
    test_for_list("clientid=c3", []),

    %% username
    test_for_list("username=u1", [<<"u1">>]),
    test_for_list("username=u3", []),

    %% peerhost
    test_for_list("peerhost=192.168.1.1", [<<"192.168.1.1">>]),
    test_for_list("peerhost=192.168.1.3", []),

    %% like clientid
    test_for_list("like_clientid=c", [<<"c1">>, <<"c2">>, <<"c[0-9]">>]),
    test_for_list("like_clientid=c3", []),

    %% like username
    test_for_list("like_username=u", [<<"u1">>, <<"u2">>, <<"u[0-9]">>]),
    test_for_list("like_username=u3", []),

    %% like peerhost
    test_for_list("like_peerhost=192.168", [<<"192.168.1.1">>, <<"192.168.1.2">>]),
    test_for_list("like_peerhost=192.168.1.3", []),

    %% like peerhost_net
    test_for_list("like_peerhost_net=192.168", [<<"192.168.0.0/16">>]),
    test_for_list("like_peerhost_net=192.166", []),

    %% with control characters
    test_for_list("like_clientid=" ++ uri_string:quote("c\\d"), [<<"c1">>, <<"c2">>]),
    ?assertMatch({error, _}, list_banned("like_clientid=???")),

    %% list all
    test_for_list([], [
        <<"c1">>,
        <<"c2">>,
        <<"u1">>,
        <<"u2">>,
        <<"192.168.1.1">>,
        <<"192.168.1.2">>,
        <<"c[0-9]">>,
        <<"u[0-9]">>,
        <<"192.168.0.0/16">>
    ]),

    %% page query with table join
    R1 = get_who_from_list("like_clientid=c&page=1&limit=1"),
    ?assertEqual(1, erlang:length(R1)),

    R2 = get_who_from_list("like_clientid=c&page=2&limit=1"),
    ?assertEqual(1, erlang:length(R2)),

    R3 = get_who_from_list("like_clientid=c&page=3&limit=1"),
    ?assertEqual(1, erlang:length(R2)),

    ?assertEqual(
        lists:sort(R1 ++ R2 ++ R3),
        lists:sort([<<"c1">>, <<"c2">>, <<"c[0-9]">>])
    ),

    emqx_banned:clear(),
    ok.

list_banned() ->
    list_banned([]).

list_banned(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["banned"]),
    case
        emqx_mgmt_api_test_util:request_api(
            get,
            Path,
            Params,
            emqx_mgmt_api_test_util:auth_header_()
        )
    of
        {ok, Apps} -> {ok, emqx_utils_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

create_banned(Banned) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["banned"]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Banned) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

delete_banned(As, Who) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["banned", As, Who]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

clear_banned() ->
    ClearPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    emqx_mgmt_api_test_util:request_api(delete, ClearPath).

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).

setup_list_test_data() ->
    emqx_banned:clear(),

    Data = [
        {clientid, <<"c1">>},
        {clientid, <<"c2">>},
        {username, <<"u1">>},
        {username, <<"u2">>},
        {peerhost, <<"192.168.1.1">>},
        {peerhost, <<"192.168.1.2">>},
        {clientid_re, <<"c[0-9]">>},
        {username_re, <<"u[0-9]">>},
        {peerhost_net, <<"192.168.0.0/16">>}
    ],

    lists:foreach(
        fun({As, Who}) ->
            {ok, Banned} = emqx_banned:parse(#{<<"as">> => As, <<"who">> => Who}),
            emqx_banned:create(Banned)
        end,
        Data
    ).

test_for_list(Params, Expected) ->
    Result = list_banned(Params),
    ?assertMatch({ok, #{<<"data">> := _}}, Result),
    {ok, #{<<"data">> := Data}} = Result,
    ?assertEqual(lists:sort(Expected), lists:sort([Who || #{<<"who">> := Who} <- Data])).

get_who_from_list(Params) ->
    Result = list_banned(Params),
    ?assertMatch({ok, #{<<"data">> := _}}, Result),
    {ok, #{<<"data">> := Data}} = Result,
    [Who || #{<<"who">> := Who} <- Data].
