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

-module(emqx_auth_clientid).

-include("emqx_auth_clientid.hrl").

-include_lib("emqx_libs/include/emqx.hrl").

%% CLI callbacks
-export([cli/1]).

%% APIs
-export([ add_clientid/2
        , update_password/2
        , lookup_clientid/1
        , remove_clientid/1
        , all_clientids/0
        ]).

-export([unwrap_salt/1]).

%% Auth callbacks
-export([ init/1
        , register_metrics/0
        , check/3
        , description/0
        ]).

-define(TAB, ?MODULE).

-record(?TAB, {clientid, password}).

%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------

cli(["list"]) ->
    ClientIds = mnesia:dirty_all_keys(?TAB),
    [emqx_ctl:print("~s~n", [ClientId]) || ClientId <- ClientIds];

cli(["add", ClientId, Password]) ->
    Ok = add_clientid(iolist_to_binary(ClientId), iolist_to_binary(Password)),
    emqx_ctl:print("~p~n", [Ok]);

cli(["update", ClientId, NewPassword]) ->
    Ok = update_password(iolist_to_binary(ClientId), iolist_to_binary(NewPassword)),
    emqx_ctl:print("~p~n", [Ok]);

cli(["del", ClientId]) ->
    emqx_ctl:print("~p~n", [remove_clientid(iolist_to_binary(ClientId))]);

cli(_) ->
    emqx_ctl:usage([{"clientid list", "List ClientId"},
                    {"clientid add <ClientId> <Password>", "Add ClientId"},
                    {"clientid update <Clientid> <NewPassword>", "Update Clientid"},
                    {"clientid del <ClientId>", "Delete ClientId"}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Add clientid with password
-spec(add_clientid(binary(), binary()) -> {atomic, ok} | {aborted, any()}).
add_clientid(ClientId, Password) ->
    Client = #?TAB{clientid = ClientId, password = encrypted_data(Password)},
    ret(mnesia:transaction(fun do_add_clientid/1, [Client])).

do_add_clientid(Client = #?TAB{clientid = ClientId}) ->
    case mnesia:read(?TAB, ClientId) of
        [] -> mnesia:write(Client);
        [_|_] -> mnesia:abort(exitsted)
    end.

%% @doc Update clientid with newpassword
-spec(update_password(binary(), binary()) -> {atomic, ok} | {aborted, any()}).
update_password(ClientId, NewPassword) ->
    Client = #?TAB{clientid = ClientId, password = encrypted_data(NewPassword)},
    ret(mnesia:transaction(fun do_update_password/1, [Client])).

do_update_password(Client = #?TAB{clientid = ClientId}) ->
    case mnesia:read(?TAB, ClientId) of
        [_|_] -> mnesia:write(Client);
        [] -> mnesia:abort(noexitsted)
    end.

%% @doc Lookup clientid
-spec(lookup_clientid(binary()) -> list(#?TAB{})).
lookup_clientid(ClientId) ->
    mnesia:dirty_read(?TAB, ClientId).

%% @doc Lookup all clientids
-spec(all_clientids() -> list(binary())).
all_clientids() ->
    mnesia:dirty_all_keys(?TAB).

%% @doc Remove clientid
-spec(remove_clientid(binary()) -> {atomic, ok} | {aborted, term()}).
remove_clientid(ClientId) ->
    ret(mnesia:transaction(fun mnesia:delete/1, [{?TAB, ClientId}])).

unwrap_salt(<<_Salt:4/binary, HashPasswd/binary>>) ->
    HashPasswd.

%% @private
ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.

%%--------------------------------------------------------------------
%% Auth callbacks
%%--------------------------------------------------------------------

init(DefaultIds) ->
    ok = ekka_mnesia:create_table(?TAB, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?TAB)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    lists:foreach(fun add_default_clientid/1, DefaultIds),
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%% @private
add_default_clientid({ClientId, Password}) ->
    add_clientid(iolist_to_binary(ClientId), iolist_to_binary(Password)).

register_metrics() ->
    [emqx_metrics:ensure(MetricName) || MetricName <- ?AUTH_METRICS].

check(#{clientid := ClientId, password := Password}, AuthResult, #{hash_type := HashType}) ->
    case mnesia:dirty_read(?TAB, ClientId) of
        [] -> emqx_metrics:inc(?AUTH_METRICS(ignore));
        [#?TAB{password = <<Salt:4/binary, Hash/binary>>}] ->
            case Hash =:= hash(Password, Salt, HashType) of
                true ->
                    emqx_metrics:inc(?AUTH_METRICS(success)),
                    {stop, AuthResult#{auth_result => success, anonymous => false}};
                false ->
                    emqx_metrics:inc(?AUTH_METRICS(failure)),
                    {stop, AuthResult#{auth_result => not_authorized, anonymous => false}}
            end
    end.

description() ->
    "ClientId Authentication Module".

encrypted_data(Password) ->
    HashType = application:get_env(emqx_auth_clientid, password_hash, sha256),
    SaltBin = salt(),
    <<SaltBin/binary, (hash(Password, SaltBin, HashType))/binary>>.

hash(undefined, SaltBin, HashType) ->
    hash(<<>>, SaltBin, HashType);
hash(Password, SaltBin, HashType) ->
    emqx_passwd:hash(HashType, <<SaltBin/binary, Password/binary>>).

salt() ->
    rand:seed(exsplus, erlang:timestamp()),
    Salt = rand:uniform(16#ffffffff), <<Salt:32>>.

