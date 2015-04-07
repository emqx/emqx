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
%%% emqttd authentication with clientid.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_clientid).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([add_clientid/1, add_clientid/2,
         lookup_clientid/1, remove_clientid/1,
         all_clientids/0]).

-behaviour(emqttd_auth).

%% emqttd_auth callbacks
-export([init/1, check/3, description/0]).

-define(AUTH_CLIENTID_TABLE, mqtt_auth_clientid).

-record(?AUTH_CLIENTID_TABLE, {clientid, password = undefined}).

add_clientid(ClientId) when is_binary(ClientId) ->
    R = #mqtt_auth_clientid{clientid = ClientId},
    mnesia:transaction(fun() -> mnesia:write(R) end).

add_clientid(ClientId, Password) ->
    R = #mqtt_auth_clientid{clientid = ClientId, password = Password},
    mnesia:transaction(fun() -> mnesia:write(R) end).

lookup_clientid(ClientId) ->
	mnesia:dirty_read(?AUTH_CLIENTID_TABLE, ClientId).

all_clientids() ->
	mnesia:dirty_all_keys(?AUTH_CLIENTID_TABLE).

remove_clientid(ClientId) ->
    mnesia:transaction(fun() -> mnesia:delete({?AUTH_CLIENTID_TABLE, ClientId}) end).

init(Opts) ->
	mnesia:create_table(?AUTH_CLIENTID_TABLE, [
        {type, set},
		{disc_copies, [node()]},
		{attributes, record_info(fields, ?AUTH_CLIENTID_TABLE)}]),
	mnesia:add_table_copy(?AUTH_CLIENTID_TABLE, node(), disc_copies),
	{ok, Opts}.

check(#mqtt_user{clientid = ClientId}, _Password, []) -> 
    check_clientid_only(ClientId);
check(#mqtt_user{clientid = ClientId}, _Password, [{password, no}|_]) -> 
    check_clientid_only(ClientId);
check(#mqtt_user{clientid = ClientId}, Password, [{password, yes}|_]) -> 
    case mnesia:dirty_read(?AUTH_CLIENTID_TABLE, ClientId) of
        [] -> {error, "ClientId Not Found"};
        [#?AUTH_CLIENTID_TABLE{password = Password}]  -> ok; %% TODO: plaintext??
        _ -> {error, "Password Not Right"}
    end.

description() -> "ClientId authentication module".

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

check_clientid_only(ClientId) ->
    case mnesia:dirty_read(?AUTH_CLIENTID_TABLE, ClientId) of
        [] -> {error, "ClientId Not Found"};
        _  -> ok
    end.


