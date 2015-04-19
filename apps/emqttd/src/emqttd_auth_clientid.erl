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

-behaviour(emqttd_auth_mod).

%% emqttd_auth callbacks
-export([init/1, check/3, description/0]).

-define(AUTH_CLIENTID_TAB, mqtt_auth_clientid).

-record(?AUTH_CLIENTID_TAB, {clientid, ipaddr, password}).

add_clientid(ClientId) when is_binary(ClientId) ->
    R = #mqtt_auth_clientid{clientid = ClientId},
    mnesia:transaction(fun() -> mnesia:write(R) end).

add_clientid(ClientId, Password) ->
    R = #mqtt_auth_clientid{clientid = ClientId, password = Password},
    mnesia:transaction(fun() -> mnesia:write(R) end).

lookup_clientid(ClientId) ->
	mnesia:dirty_read(?AUTH_CLIENTID_TAB, ClientId).

all_clientids() ->
	mnesia:dirty_all_keys(?AUTH_CLIENTID_TAB).

remove_clientid(ClientId) ->
    mnesia:transaction(fun() -> mnesia:delete({?AUTH_CLIENTID_TAB, ClientId}) end).

init(Opts) ->
	mnesia:create_table(?AUTH_CLIENTID_TAB, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, ?AUTH_CLIENTID_TAB)}]),
	mnesia:add_table_copy(?AUTH_CLIENTID_TAB, node(), ram_copies),
    case proplists:get_value(file, Opts) of
        undefined -> ok;
        File -> load(File)
    end,
	{ok, Opts}.

check(#mqtt_client{clientid = undefined}, _Password, []) ->
    {error, "ClientId undefined"};
check(#mqtt_client{clientid = ClientId, ipaddr = IpAddr}, _Password, []) ->
    check_clientid_only(ClientId, IpAddr);
check(#mqtt_client{clientid = ClientId, ipaddr = IpAddr}, _Password, [{password, no}|_]) ->
    check_clientid_only(ClientId, IpAddr);
check(_Client, undefined, [{password, yes}|_]) ->
    {error, "Password undefined"};
check(#mqtt_client{clientid = ClientId}, Password, [{password, yes}|_]) ->
    case mnesia:dirty_read(?AUTH_CLIENTID_TAB, ClientId) of
        [] -> {error, "ClientId Not Found"};
        [#?AUTH_CLIENTID_TAB{password = Password}]  -> ok; %% TODO: plaintext??
        _ -> {error, "Password Not Right"}
    end.

description() -> "ClientId authentication module".

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

load(File) ->
    {ok, Fd} = file:open(File, [read]),
    load(Fd, file:read_line(Fd), []).

load(Fd, {ok, Line}, Clients) when is_list(Line) ->
    Clients1 =
    case string:tokens(Line, " ") of
        [ClientIdS] ->
            ClientId = list_to_binary(string:strip(ClientIdS, right, $\n)),
            [#mqtt_auth_clientid{clientid = ClientId} | Clients];
        [ClientId, IpAddr0] ->
            IpAddr = string:strip(IpAddr0, right, $\n),
            Range = esockd_access:range(IpAddr),
            [#mqtt_auth_clientid{clientid = list_to_binary(ClientId),
                                 ipaddr = {IpAddr, Range}}|Clients];
        BadLine ->
            lager:error("BadLine in clients.config: ~s", [BadLine]),
            Clients
    end,
    load(Fd, file:read_line(Fd), Clients1);

load(Fd, eof, Clients) -> 
    mnesia:transaction(fun() -> [mnesia:write(C) || C<- Clients] end),
    file:close(Fd).

check_clientid_only(ClientId, IpAddr) ->
    case mnesia:dirty_read(?AUTH_CLIENTID_TAB, ClientId) of
        [] -> {error, "ClientId Not Found"};
        [#?AUTH_CLIENTID_TAB{ipaddr = undefined}]  -> ok;
        [#?AUTH_CLIENTID_TAB{ipaddr = {_, {Start, End}}}] ->
            I = esockd_access:atoi(IpAddr),
            case I >= Start andalso I =< End of
                true -> ok;
                false -> {error, "ClientId with wrong IP address"}
            end
    end.


