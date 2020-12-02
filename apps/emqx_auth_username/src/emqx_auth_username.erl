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

-module(emqx_auth_username).

-include("emqx_auth_username.hrl").
-include_lib("emqx_libs/include/emqx.hrl").

%% CLI callbacks
-export([cli/1]).

%% APIs
-export([ add_user/2
        , update_password/2
        , remove_user/1
        , lookup_user/1
        , all_users/0
        ]).

-export([unwrap_salt/1]).

%% Auth callbacks
-export([ init/1
        , register_metrics/0
        , check/3
        , description/0
        ]).

-define(TAB, ?MODULE).

-record(?TAB, {username, password}).

%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------

cli(["list"]) ->
    Usernames = mnesia:dirty_all_keys(?TAB),
    [emqx_ctl:print("~s~n", [Username]) || Username <- Usernames];

cli(["add", Username, Password]) ->
    Ok = add_user(iolist_to_binary(Username), iolist_to_binary(Password)),
    emqx_ctl:print("~p~n", [Ok]);

cli(["update", Username, NewPassword]) ->
    Ok = update_password(iolist_to_binary(Username), iolist_to_binary(NewPassword)),
    emqx_ctl:print("~p~n", [Ok]);

cli(["del", Username]) ->
    emqx_ctl:print("~p~n", [remove_user(iolist_to_binary(Username))]);

cli(_) ->
    emqx_ctl:usage([{"users list", "List users"},
                    {"users add <Username> <Password>", "Add User"},
                    {"users update <Username> <NewPassword>", "Update User"},
                    {"users del <Username>", "Delete User"}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Add User
-spec(add_user(binary(), binary()) -> ok | {error, any()}).
add_user(Username, Password) ->
    User = #?TAB{username = Username, password = encrypted_data(Password)},
    ret(mnesia:transaction(fun insert_user/1, [User])).

insert_user(User = #?TAB{username = Username}) ->
    case mnesia:read(?TAB, Username) of
        []    -> mnesia:write(User);
        [_|_] -> mnesia:abort(existed)
    end.

%% @doc Update User
-spec(update_password(binary(), binary()) -> ok | {error, any()}).
update_password(Username, NewPassword) ->
    User = #?TAB{username = Username, password = encrypted_data(NewPassword)},
    ret(mnesia:transaction(fun do_update_password/1, [User])).

do_update_password(User = #?TAB{username = Username}) ->
    case mnesia:read(?TAB, Username) of
        [_|_] -> mnesia:write(User);
        [] -> mnesia:abort(noexisted)
    end.

%% @doc Lookup user by username
-spec(lookup_user(binary()) -> list()).
lookup_user(Username) ->
    mnesia:dirty_read(?TAB, Username).

%% @doc Remove user
-spec(remove_user(binary()) -> ok | {error, any()}).
remove_user(Username) ->
    ret(mnesia:transaction(fun mnesia:delete/1, [{?TAB, Username}])).

ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.

%% @doc All usernames
-spec(all_users() -> list()).
all_users() -> mnesia:dirty_all_keys(?TAB).

unwrap_salt(<<_Salt:4/binary, HashPasswd/binary>>) ->
    HashPasswd.

%%--------------------------------------------------------------------
%% Auth callbacks
%%--------------------------------------------------------------------

init(DefaultUsers) ->
    ok = ekka_mnesia:create_table(?TAB, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?TAB)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    ok = lists:foreach(fun add_default_user/1, DefaultUsers),
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%% @private
add_default_user({Username, Password}) ->
    add_user(iolist_to_binary(Username), iolist_to_binary(Password)).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTH_METRICS).

check(#{username := Username, password := Password}, AuthResult, #{hash_type := HashType}) ->
    case mnesia:dirty_read(?TAB, Username) of
        [] -> emqx_metrics:inc(?AUTH_METRICS(ignore));
        [#?TAB{password = <<Salt:4/binary, Hash/binary>>}] ->
            case Hash =:= hash(Password, Salt, HashType) of
                true ->
                    ok = emqx_metrics:inc(?AUTH_METRICS(success)),
                    {stop, AuthResult#{auth_result => success, anonymous => false}};
                false ->
                    ok = emqx_metrics:inc(?AUTH_METRICS(failure)),
                    {stop, AuthResult#{auth_result => not_authorized, anonymous => false}}
            end
    end.

description() ->
    "Username password Authentication Module".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

encrypted_data(Password) ->
    HashType = application:get_env(emqx_auth_username, password_hash, sha256),
    SaltBin = salt(),
    <<SaltBin/binary, (hash(Password, SaltBin, HashType))/binary>>.

hash(undefined, SaltBin, HashType) ->
    hash(<<>>, SaltBin, HashType);
hash(Password, SaltBin, HashType) ->
    emqx_passwd:hash(HashType, <<SaltBin/binary, Password/binary>>).

salt() ->
    rand:seed(exsplus, erlang:timestamp()),
    Salt = rand:uniform(16#ffffffff), <<Salt:32>>.

