%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_banned).

-include_lib("emqx/include/emqx.hrl").

%% API
-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_create_clientid/1
        , handle_create_username/1
        , handle_create_peerhost/1
        , handle_delete_clientid/1
        , handle_delete_username/1
        , handle_delete_peerhost/1]).

rest_schema() ->
    DefinitionName = <<"banned">>,
    DefinitionProperties = #{
        <<"who">> => #{
            type => <<"string">>,
            description => <<"Client ID, Username or IP">>},
        <<"by">> => #{
            type => <<"string">>,
            description => <<"Indicate which object was added to the blacklist">>},
        <<"reason">> => #{
            type => <<"string">>,
            description => <<"Banned Reason">>},
        <<"at">> => #{
            type => <<"integer">>,
            description => <<"Time added to blacklist, unit: second">>,
            example => erlang:system_time(second)},
        <<"until">> => #{
            type => <<"integer">>,
            description => <<"When to remove from blacklist, unit: second">>,
            example => erlang:system_time(second) + 300}},
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [banned_api()] ++ create_banned_api() ++ cancel_banned_api().

banned_api() ->
    Metadata = #{
        get => #{
            tags => ["client"],
            description => "EMQ X banned list",
            operationId => handle_list,
            parameters =>
                [#{
                    name => page,
                    in => query,
                    description => <<"Page">>,
                    type => integer,
                    default => 1
                },
                #{
                    name => limit,
                    in => query,
                    description => <<"Page size">>,
                    type => integer,
                    default => emqx_mgmt:max_row_limit()
                }],
            responses => #{
                <<"200">> => #{
                    schema => #{
                        type => array,
                        items => cowboy_swagger:schema(<<"banned">>)}}},
            security => [#{application => []}]}},
    {"/banned", Metadata}.

create_banned_api() ->
    [create_banned_api(Type) || Type <- ["clientid", "username", "peerhost"]].

create_banned_api(Type) ->
    Metadata = #{
        post => #{
            tags => ["client"],
            description => "EMQ X banned",
            operationId => list_to_atom("handle_create_" ++ Type),
            parameters =>
            [
                #{
                    name => Type,
                    in => path,
                    required => true,
                    description => "Banned " ++ Type,
                    type => string
                },
                #{
                    name => banned,
                    in => body,
                    schema => cowboy_swagger:schema(<<"banned">>)
                }
            ],
            responses => #{<<"200">> => #{description => <<"ok">>}},
            security => [#{application => []}]}},
    {"/banned/" ++ Type ++ "/:" ++ Type, Metadata}.

cancel_banned_api() ->
    [cancel_banned_api(Type) || Type <- ["clientid", "username", "peerhost"]].

cancel_banned_api(Type) ->
    Metadata = #{
        delete => #{
            tags => ["client"],
            description => "EMQ X cancel banned",
            operationId => list_to_atom("handle_delete_" ++ Type),
            parameters =>
            [#{
                name => Type,
                in => path,
                required => true,
                description => Type,
                type => string
            }],
            responses => #{<<"200">> => #{description => <<"ok">>}},
            security => [#{application => []}]}},
    {"/banned/" ++ Type ++ "/:" ++ Type, Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(Request) ->
    Params = cowboy_req:parse_qs(Request),
    list(Params).

handle_create_clientid(Request) ->
    handle_create(clientid, Request).

handle_create_username(Request) ->
    handle_create(username, Request).

handle_create_peerhost(Request) ->
    handle_create(peerhost, Request).

handle_create(Type, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Banned = emqx_json:decode(Body, [return_maps]),
    Params = #{
        as      => Type,
        who     => maps:get(<<"who">>, Banned),
        by      => maps:get(<<"by">>, Banned, undefined),
        at      => maps:get(<<"at">>, Banned, undefined),
        until   => maps:get(<<"until">>, Banned, undefined),
        reason  => maps:get(<<"reason">>, Banned, undefined)
    },
    create(Params).

handle_delete_clientid(Request) ->
    Who = cowboy_req:binding(who, Request),
    delete_banned_clientid(#{who => Who}).

handle_delete_username(Request) ->
    Who = cowboy_req:binding(who, Request),
    delete_banned_username(#{who => Who}).

handle_delete_peerhost(Request) ->
    Who = cowboy_req:binding(who, Request),
    delete_banned_peerhost(#{who => Who}).

%%%==============================================================================================
%% api apply
list(Params) ->
    Data = emqx_mgmt_api:paginate(emqx_banned, Params, fun format/1),
    Response = emqx_json:encode(Data),
    {200, Response}.

create(Params) ->
    Banned = pack_banned(Params),
    ok = emqx_mgmt:create_banned(Banned),
    {200}.

delete_banned_clientid(#{who := Who}) ->
    ok = emqx_mgmt:delete_banned({clientid, Who}),
    {200}.

delete_banned_username(#{who := Who}) ->
    ok = emqx_mgmt:delete_banned({username, Who}),
    {200}.

delete_banned_peerhost(#{who := Who}) ->
    {ok, IPAddress} = inet:parse_address(binary_to_list(Who)),
    ok = emqx_mgmt:delete_banned({peerhost, IPAddress}),
    {200}.

%%%==============================================================================================
%% internal
format(BannedList) when is_list(BannedList) ->
    [format(Ban) || Ban <- BannedList];
format(#banned{who = {As, Who}, by = By, reason = Reason, at = At, until = Until}) ->
    #{who => case As of
                 peerhost -> bin(inet:ntoa(Who));
                 _ -> Who
             end,
        as => As, by => By, reason => Reason, at => At, until => Until}.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

pack_banned(BannedMap = #{who := Who}) ->
    Now = erlang:system_time(second),
    Fun =
        fun (by, undefined, Banned) ->
            Banned#banned{by = <<"user">>};
            (by, By, Banned) ->
                Banned#banned{by = By};
            (at, undefined, Banned) ->
                Banned#banned{at = Now};
            (at, At, Banned) ->
                Banned#banned{at = At};
            (until, undefined, Banned) ->
                Banned#banned{until = Now + 300};
            (as, <<"peerhost">>, Banned) ->
                {ok, IPAddress} = inet:parse_address(binary_to_list(Who)),
                Banned#banned{who = {peerhost, IPAddress}};
            (as, <<"clientid">>, Banned) ->
                Banned#banned{who = {clientid, Who}};
            (as, <<"username">>, Banned) ->
                Banned#banned{who = {username, Who}};
            (reason, Reason, Banned) ->
                Banned#banned{reason = Reason};
            (until, Until, Banned) ->
                Banned#banned{until = Until};
            (_key, _Value, Banned) ->
                Banned
        end,
    maps:fold(Fun, #banned{}, BannedMap).
