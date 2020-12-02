%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_libs/include/emqx.hrl").

-include("emqx_mgmt.hrl").

-import(proplists, [get_value/2]).

-import(minirest, [ return/0
                  , return/1
                  ]).

-rest_api(#{name   => list_banned,
            method => 'GET',
            path   => "/banned/",
            func   => list,
            descr  => "List banned"}).

-rest_api(#{name   => create_banned,
            method => 'POST',
            path   => "/banned/",
            func   => create,
            descr  => "Create banned"}).

-rest_api(#{name   => delete_banned,
            method => 'DELETE',
            path   => "/banned/:as/:who",
            func   => delete,
            descr  => "Delete banned"}).

-export([ list/2
        , create/2
        , delete/2
        ]).

list(_Bindings, Params) ->
    return({ok, emqx_mgmt_api:paginate(emqx_banned, Params, fun format/1)}).

create(_Bindings, Params) ->
    case pipeline([fun ensure_required/1,
                   fun validate_params/1], Params) of
        {ok, NParams} ->
            {ok, Banned} = pack_banned(NParams),
            ok = emqx_mgmt:create_banned(Banned),
            return({ok, maps:from_list(Params)});
        {error, Code, Message} -> 
            return({error, Code, Message})
    end.

delete(#{as := As, who := Who}, _) ->
    Params = [{<<"who">>, bin(emqx_mgmt_util:urldecode(Who))},
              {<<"as">>, bin(emqx_mgmt_util:urldecode(As))}],
    case pipeline([fun ensure_required/1,
                   fun validate_params/1], Params) of
        {ok, NParams} ->
            do_delete(get_value(<<"as">>, NParams), get_value(<<"who">>, NParams)),
            return();
        {error, Code, Message} -> 
            return({error, Code, Message})
    end.

pipeline([], Params) ->
    {ok, Params};
pipeline([Fun|More], Params) ->
    case Fun(Params) of
        {ok, NParams} ->
            pipeline(More, NParams);
        {error, Code, Message} ->
            {error, Code, Message}
    end.

%% Plugs
ensure_required(Params) when is_list(Params) ->
    #{required_params := RequiredParams, message := Msg} = required_params(),
    AllIncluded = lists:all(fun(Key) ->
                      lists:keymember(Key, 1, Params)
                  end, RequiredParams),
    case AllIncluded of
        true -> {ok, Params};
        false ->
            {error, ?ERROR7, Msg}
    end.

validate_params(Params) ->
    #{enum_values := AsEnums, message := Msg} = enum_values(as),
    case lists:member(get_value(<<"as">>, Params), AsEnums) of
        true -> {ok, Params};
        false ->
            {error, ?ERROR8, Msg}
    end.

pack_banned(Params) ->
    Now = erlang:system_time(second),
    do_pack_banned(Params, #banned{by = <<"user">>,
                                   at = Now,
                                   until = Now + 300}).

do_pack_banned([], Banned) ->
    {ok, Banned};
do_pack_banned([{<<"who">>, Who} | Params], Banned) ->
    case lists:keytake(<<"as">>, 1, Params) of
        {value, {<<"as">>, <<"peerhost">>}, Params2} ->
            {ok, IPAddress} = inet:parse_address(str(Who)),
            do_pack_banned(Params2, Banned#banned{who = {peerhost, IPAddress}});
        {value, {<<"as">>, <<"clientid">>}, Params2} ->
            do_pack_banned(Params2, Banned#banned{who = {clientid, Who}});
        {value, {<<"as">>, <<"username">>}, Params2} ->
            do_pack_banned(Params2, Banned#banned{who = {username, Who}})
    end;
do_pack_banned([P1 = {<<"as">>, _}, P2 | Params], Banned) ->
    do_pack_banned([P2, P1 | Params], Banned);
do_pack_banned([{<<"by">>, By} | Params], Banned) ->
    do_pack_banned(Params, Banned#banned{by = By});
do_pack_banned([{<<"reason">>, Reason} | Params], Banned) ->
    do_pack_banned(Params, Banned#banned{reason = Reason});
do_pack_banned([{<<"at">>, At} | Params], Banned) ->
    do_pack_banned(Params, Banned#banned{at = At});
do_pack_banned([{<<"until">>, Until} | Params], Banned) ->
    do_pack_banned(Params, Banned#banned{until = Until});
do_pack_banned([_P | Params], Banned) -> %% ingore other params
    do_pack_banned(Params, Banned).

do_delete(<<"peerhost">>, Who) ->
    {ok, IPAddress} = inet:parse_address(str(Who)),
    emqx_mgmt:delete_banned({peerhost, IPAddress});
do_delete(<<"username">>, Who) ->
    emqx_mgmt:delete_banned({username, bin(Who)});
do_delete(<<"clientid">>, Who) ->
    emqx_mgmt:delete_banned({clientid, bin(Who)}).

required_params() ->
    #{required_params => [<<"who">>, <<"as">>],
      message => <<"missing mandatory params: ['who', 'as']">> }.

enum_values(as) ->
    #{enum_values => [<<"clientid">>, <<"username">>, <<"peerhost">>],
      message => <<"value of 'as' must be one of: ['clientid', 'username', 'peerhost']">> }.

%% Internal Functions

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

str(B) when is_binary(B) ->
    binary_to_list(B);
str(L) when is_list(L) ->
    L.
