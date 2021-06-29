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
        , handle_post/1
        , handle_delete/1]).

rest_schema() ->
    DefinitionName = <<"banned">>,
    DefinitionProperties = #{
        <<"who">> =>
        #{type => <<"string">>, description => <<"Client ID, Username or IP">>},
        <<"as">> =>
        #{type => <<"string">>, enum => [clientid, username, peerhost], description => <<"Type of who">>},
        <<"by">> =>
        #{type => <<"string">>, description => <<"Indicate which object was added to the blacklist">>,
            required => false},
        <<"reason">> =>
        #{type => <<"string">>, description => <<"Banned Reason">>,
            required => false},
        <<"at">> =>
        #{type => <<"integer">>, description => <<"Time added to blacklist, unit: second">>,
            required => false, example => erlang:system_time(second)},
        <<"until">> =>
        #{type => <<"integer">>, description => <<"When to remove from blacklist, unit: second">>,
            required => false, example => erlang:system_time(second) + 300}
        },
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [banned_api(), cancel_banned_api()].

banned_api() ->
    Metadata = #{
        get =>
        #{tags => ["client"],
            description => "EMQ X banned list",
            operationId => handle_list,
            parameters => [
                #{name => page
                , in => query
                , description => <<"Page">>
                , required => true
                , schema =>
                #{type => integer, default => 1}},
                #{name => limit
                , in => query
                , description => <<"Page size">>
                , required => true
                , schema =>
                #{type => integer, default => emqx_mgmt:max_row_limit()}}
            ],
            responses => #{
                <<"200">> => #{
                    content => #{
                        'application/json' =>
                        #{schema => #{
                            type => array,
                            items => cowboy_swagger:schema(<<"banned">>)}}}}}},
        post =>
        #{tags => ["client"],
            description => "EMQ X banned",
            operationId => handle_post,
            requestBody => #{
                content => #{
                    'application/json' => #{schema => cowboy_swagger:schema(<<"banned">>)}}},
            responses => #{
                <<"200">> => #{description => <<"ok">>}}}},
    {"/banned", Metadata}.

cancel_banned_api() ->
    Metadata = #{
        delete =>
        #{tags => ["client"],
        description => "EMQ X cancel banned",
        operationId => handle_delete,
        parameters => [
            #{name => as
            , in => query
            , required => true
            , schema =>
            #{type => string, enum => [clientid, username, peerhost], default => clientid}},
            #{name => who
            , in => query
            , description => <<"Clientid or username or peerhost">>
            , required => true
            , schema =>
            #{type => string, default => <<"123456">>}}],
        responses => #{
            <<"200">> => #{description => <<"ok">>}}}},
    {"/banned/:as/:who", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(Request) ->
    Params = cowboy_req:parse_qs(Request),
    list(Params).

handle_post(Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Banned = emqx_json:decode(Body, [return_maps]),
    Params = #{
        who     => maps:get(<<"who">>, Banned),
        as      => maps:get(<<"as">>, Banned),
        by      => maps:get(<<"by">>, Banned, undefined),
        at      => maps:get(<<"at">>, Banned, undefined),
        until   => maps:get(<<"until">>, Banned, undefined),
        reason  => maps:get(<<"reason">>, Banned, undefined)
    },
    create(Params).

handle_delete(Request) ->
    QParams = cowboy_req:parse_qs(Request),
    As = proplists:get_value(<<"as">>, QParams),
    Who = proplists:get_value(<<"who">>, QParams),
    delete(#{as => As, who => Who}).

%%%==============================================================================================
%% api apply
list(Params) ->
    Data = emqx_mgmt_api:paginate(emqx_banned, Params, fun format/1),
    Response = emqx_json:encode(Data),
    {ok, Response}.

create(Params) ->
    Banned = pack_banned(Params),
    ok = emqx_mgmt:create_banned(Banned),
    {ok}.

delete(#{as := As, who := Who}) ->
    ok = emqx_mgmt:delete_banned(trans_who(As, Who)),
    {ok}.

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
            (at, undefined, Banned) ->
                Banned#banned{at = Now};
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
            (by, By, Banned) ->
                Banned#banned{by = By};
            (at, At, Banned) ->
                Banned#banned{at = At};
            (until, Until, Banned) ->
                Banned#banned{until = Until};
            (_key, _Value, Banned) ->
                Banned
        end,
    maps:fold(Fun, #banned{}, BannedMap).

trans_who(<<"clientid">>, Who) -> {clientid, Who};
trans_who(<<"peerhost">>, Who) ->
    {ok, IPAddress} = inet:parse_address(binary_to_list(Who)),
    {peerhost, IPAddress};
trans_who(<<"username">>, Who) -> {username, Who}.

