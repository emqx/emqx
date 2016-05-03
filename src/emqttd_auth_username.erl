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

%% @doc Authentication with username and password
-module(emqttd_auth_username).

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

%% CLI callbacks
-export([cli/1]).

-behaviour(emqttd_auth_mod).

-export([is_enabled/0]).

-export([add_user/2, remove_user/1, lookup_user/1, all_users/0]).

%% emqttd_auth callbacks
-export([init/1, check/3, description/0]).

-define(AUTH_USERNAME_TAB, mqtt_auth_username).

-record(?AUTH_USERNAME_TAB, {username, password}).

%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------

cli(["add", Username, Password]) ->
    if_enabled(fun() ->
        ?PRINT("~p~n", [add_user(iolist_to_binary(Username), iolist_to_binary(Password))])
    end);

cli(["del", Username]) ->
    if_enabled(fun() ->
        ?PRINT("~p~n", [remove_user(iolist_to_binary(Username))])
    end);

cli(_) ->
    ?USAGE([{"users add <Username> <Password>", "Add User"},
            {"users del <Username>", "Delete User"}]).

if_enabled(Fun) ->
    case is_enabled() of
        true  -> Fun();
        false -> hint()
    end.

hint() ->
    ?PRINT_MSG("Please enable '{username, []}' authentication in etc/emqttd.config first.~n").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

is_enabled() ->
    lists:member(?AUTH_USERNAME_TAB, mnesia:system_info(tables)).

%% @doc Add User
-spec(add_user(binary(), binary()) -> ok | {error, any()}).
add_user(Username, Password) ->
    User = #?AUTH_USERNAME_TAB{username = Username, password = hash(Password)},
    ret(mnesia:transaction(fun mnesia:write/1, [User])).

add_default_user(Username, Password) when is_atom(Username) ->
    add_default_user(atom_to_list(Username), Password);

add_default_user(Username, Password) ->
    add_user(iolist_to_binary(Username), iolist_to_binary(Password)).

%% @doc Lookup user by username
-spec(lookup_user(binary()) -> list()).
lookup_user(Username) ->
    mnesia:dirty_read(?AUTH_USERNAME_TAB, Username).

%% @doc Remove user
-spec(remove_user(binary()) -> ok | {error, any()}).
remove_user(Username) ->
    ret(mnesia:transaction(fun mnesia:delete/1, [{?AUTH_USERNAME_TAB, Username}])).

ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.

%% @doc All usernames
-spec(all_users() -> list()).
all_users() -> mnesia:dirty_all_keys(?AUTH_USERNAME_TAB).

%%--------------------------------------------------------------------
%% emqttd_auth_mod callbacks
%%--------------------------------------------------------------------

init(DefautUsers) ->
    mnesia:create_table(?AUTH_USERNAME_TAB, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?AUTH_USERNAME_TAB)}]),
    mnesia:add_table_copy(?AUTH_USERNAME_TAB, node(), disc_copies),
    lists:foreach(fun({Username, Password}) ->
                add_default_user(Username, Password)
        end, DefautUsers),
    emqttd_ctl:register_cmd(users, {?MODULE, cli}, []),
    {ok, []}.

check(#mqtt_client{username = undefined}, _Password, _Opts) ->
    {error, username_undefined};
check(_User, undefined, _Opts) ->
    {error, password_undefined};
check(#mqtt_client{username = Username}, Password, _Opts) ->
    case mnesia:dirty_read(?AUTH_USERNAME_TAB, Username) of
        [] -> 
            {error, username_not_found};
        [#?AUTH_USERNAME_TAB{password = <<Salt:4/binary, Hash/binary>>}] ->
            case Hash =:= md5_hash(Salt, Password) of
                true -> ok;
                false -> {error, password_error}
            end
    end.

description() ->
    "Username password authentication module".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

hash(Password) ->
    SaltBin = salt(), <<SaltBin/binary, (md5_hash(SaltBin, Password))/binary>>.

md5_hash(SaltBin, Password) ->
    erlang:md5(<<SaltBin/binary, Password/binary>>).

salt() ->
    emqttd_time:seed(), Salt = random:uniform(16#ffffffff), <<Salt:32>>.

