%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_auth_clientid).

-include("emqttd.hrl").

-export([add_clientid/1, add_clientid/2, lookup_clientid/1, remove_clientid/1,
         all_clientids/0]).

-behaviour(emqttd_auth_mod).

%% emqttd_auth_mod callbacks
-export([init/1, check/3, description/0]).

-define(AUTH_CLIENTID_TAB, mqtt_auth_clientid).

-record(?AUTH_CLIENTID_TAB, {client_id, ipaddr, password}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Add clientid
-spec(add_clientid(binary()) -> {atomic, ok} | {aborted, any()}).
add_clientid(ClientId) when is_binary(ClientId) ->
    R = #mqtt_auth_clientid{client_id = ClientId},
    mnesia:transaction(fun mnesia:write/1, [R]).

%% @doc Add clientid with password
-spec(add_clientid(binary(), binary()) -> {atomic, ok} | {aborted, any()}).
add_clientid(ClientId, Password) ->
    R = #mqtt_auth_clientid{client_id = ClientId, password = Password},
    mnesia:transaction(fun mnesia:write/1, [R]).

%% @doc Lookup clientid
-spec(lookup_clientid(binary()) -> list(#mqtt_auth_clientid{})).
lookup_clientid(ClientId) ->
    mnesia:dirty_read(?AUTH_CLIENTID_TAB, ClientId).

%% @doc Lookup all clientids
-spec(all_clientids() -> list(binary())).
all_clientids() -> mnesia:dirty_all_keys(?AUTH_CLIENTID_TAB).

%% @doc Remove clientid
-spec(remove_clientid(binary()) -> {atomic, ok} | {aborted, any()}).
remove_clientid(ClientId) ->
    mnesia:transaction(fun mnesia:delete/1, [{?AUTH_CLIENTID_TAB, ClientId}]).

%%--------------------------------------------------------------------
%% emqttd_auth_mod callbacks
%%--------------------------------------------------------------------

init(Opts) ->
    mnesia:create_table(?AUTH_CLIENTID_TAB, [
            {ram_copies, [node()]},
            {attributes, record_info(fields, ?AUTH_CLIENTID_TAB)}]),
    mnesia:add_table_copy(?AUTH_CLIENTID_TAB, node(), ram_copies),
    load(proplists:get_value(file, Opts)),
    {ok, Opts}.

check(#mqtt_client{client_id = undefined}, _Password, _Opts) ->
    {error, clientid_undefined};
check(#mqtt_client{client_id = ClientId, peername = {IpAddress, _}}, _Password, []) ->
    check_clientid_only(ClientId, IpAddress);
check(#mqtt_client{client_id = ClientId, peername = {IpAddress, _}}, _Password, [{password, no}|_]) ->
    check_clientid_only(ClientId, IpAddress);
check(_Client, undefined, [{password, yes}|_]) ->
    {error, password_undefined};
check(#mqtt_client{client_id = ClientId}, Password, [{password, yes}|_]) ->
    case mnesia:dirty_read(?AUTH_CLIENTID_TAB, ClientId) of
        [] -> {error, clientid_not_found};
        [#?AUTH_CLIENTID_TAB{password = Password}]  -> ok; %% TODO: plaintext??
        _ -> {error, password_error}
    end.

description() -> "ClientId authentication module".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

load(undefined) ->
    ok;

load(File) ->
    {ok, Fd} = file:open(File, [read]),
    load(Fd, file:read_line(Fd), []).

load(Fd, {ok, Line}, Clients) when is_list(Line) ->
    Clients1 =
    case string:tokens(Line, " ") of
        [ClientIdS] ->
            ClientId = list_to_binary(string:strip(ClientIdS, right, $\n)),
            [#mqtt_auth_clientid{client_id = ClientId} | Clients];
        [ClientId, IpAddr0] ->
            IpAddr = string:strip(IpAddr0, right, $\n),
            [#mqtt_auth_clientid{client_id = list_to_binary(ClientId),
                                 ipaddr = esockd_cidr:parse(IpAddr, true)} | Clients];
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
        [] ->
            {error, clientid_not_found};
        [#?AUTH_CLIENTID_TAB{ipaddr = undefined}] ->
            ok;
        [#?AUTH_CLIENTID_TAB{ipaddr = CIDR}] ->
            case esockd_cidr:match(IpAddr, CIDR) of
                true  -> ok;
                false -> {error, wrong_ipaddr}
            end
    end.

