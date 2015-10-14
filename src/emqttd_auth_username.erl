%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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
%%% Authentication with username and password.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_username).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

%% CLI callbacks
-export([cli/1]).

-behaviour(emqttd_auth_mod).

-export([add_user/2, remove_user/1,
         lookup_user/1, all_users/0]).

%% emqttd_auth callbacks
-export([init/1, check/3, description/0]).

-define(AUTH_USERNAME_TAB, mqtt_auth_username).

-record(?AUTH_USERNAME_TAB, {username, password}).

%%%=============================================================================
%%% CLI
%%%=============================================================================

cli(["add", Username, Password]) ->
    ?PRINT("~p~n", [add_user(list_to_binary(Username), list_to_binary(Password))]);

cli(["del", Username]) ->
    ?PRINT("~p~n", [remove_user(list_to_binary(Username))]);

cli(_) ->
    ?USAGE([{"users add <Username> <Password>", "add user"},
            {"users del <Username>", "delete user"}]).

%%%=============================================================================
%%% API 
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Add user
%% @end
%%------------------------------------------------------------------------------
-spec add_user(binary(), binary()) -> {atomic, ok} | {aborted, any()}.
add_user(Username, Password) ->
    User = #?AUTH_USERNAME_TAB{username = Username, password = hash(Password)},
    mnesia:transaction(fun mnesia:write/1, [User]).

%%------------------------------------------------------------------------------
%% @doc Lookup user by username
%% @end
%%------------------------------------------------------------------------------
-spec lookup_user(binary()) -> list().
lookup_user(Username) ->
    mnesia:dirty_read(?AUTH_USERNAME_TAB, Username).

%%------------------------------------------------------------------------------
%% @doc Remove user
%% @end
%%------------------------------------------------------------------------------
-spec remove_user(binary()) -> {atomic, ok} | {aborted, any()}.
remove_user(Username) ->
    mnesia:transaction(fun mnesia:delete/1, [{?AUTH_USERNAME_TAB, Username}]).

%%------------------------------------------------------------------------------
%% @doc All usernames
%% @end
%%------------------------------------------------------------------------------
-spec all_users() -> list().
all_users() ->
    mnesia:dirty_all_keys(?AUTH_USERNAME_TAB).

%%%=============================================================================
%%% emqttd_auth callbacks
%%%=============================================================================
init(Opts) ->
	mnesia:create_table(?AUTH_USERNAME_TAB, [
		{disc_copies, [node()]},
		{attributes, record_info(fields, ?AUTH_USERNAME_TAB)}]),
	mnesia:add_table_copy(?AUTH_USERNAME_TAB, node(), ram_copies),
    emqttd_ctl:register_cmd(users, {?MODULE, cli}, []),
    {ok, Opts}.

check(#mqtt_client{username = undefined}, _Password, _Opts) ->
    {error, "Username undefined"};
check(_User, undefined, _Opts) ->
    {error, "Password undefined"};
check(#mqtt_client{username = Username}, Password, _Opts) ->
	case mnesia:dirty_read(?AUTH_USERNAME_TAB, Username) of
        [] -> 
            {error, "Username Not Found"};
        [#?AUTH_USERNAME_TAB{password = <<Salt:4/binary, Hash/binary>>}] ->
            case Hash =:= md5_hash(Salt, Password) of
                true -> ok;
                false -> {error, "Password Not Right"}
            end
	end.
	
description() ->
    "Username password authentication module".

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

hash(Password) ->
    SaltBin = salt(),
    <<SaltBin/binary, (md5_hash(SaltBin, Password))/binary>>.

md5_hash(SaltBin, Password) ->
    erlang:md5(<<SaltBin/binary, Password/binary>>).

salt() ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Salt = random:uniform(16#ffffffff),
    <<Salt:32>>.

