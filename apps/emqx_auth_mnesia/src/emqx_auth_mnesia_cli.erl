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

-module(emqx_auth_mnesia_cli).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx/include/logger.hrl").
-define(TABLE, emqx_user).
%% Auth APIs
-export([ add_user/3
        , update_user/3
        , remove_user/1
        , lookup_user/1
        , all_users/0
        ]).
%% Acl APIs
-export([ add_acl/4
        , remove_acl/2
        , lookup_acl/1
        , all_acls/0
        ]).
%% Cli
-export([ auth_cli/1
        , acl_cli/1]).
%%--------------------------------------------------------------------
%% Auth APIs
%%--------------------------------------------------------------------

%% @doc Add User
-spec(add_user(binary(), binary(), atom()) -> ok | {error, any()}).
add_user(Login, Password, IsSuperuser) ->
    User = #emqx_user{login = Login, password = encrypted_data(Password), is_superuser = IsSuperuser},
    ret(mnesia:transaction(fun insert_user/1, [User])).

insert_user(User = #emqx_user{login = Login}) ->
    case mnesia:read(?TABLE, Login) of
        []    -> mnesia:write(User);
        [_|_] -> mnesia:abort(existed)
    end.

%% @doc Update User
-spec(update_user(binary(), binary(), atom()) -> ok | {error, any()}).
update_user(Login, NewPassword, IsSuperuser) ->
    User = #emqx_user{login = Login, password = encrypted_data(NewPassword), is_superuser = IsSuperuser},
    ret(mnesia:transaction(fun do_update_user/1, [User])).

do_update_user(User = #emqx_user{login = Login}) ->
    case mnesia:read(?TABLE, Login) of
        [_|_] -> mnesia:write(User);
        [] -> mnesia:abort(noexisted)
    end.

%% @doc Lookup user by login
-spec(lookup_user(binary()) -> list()).
lookup_user(undefined) -> [];
lookup_user(Login) ->
    case mnesia:dirty_read(?TABLE, Login) of
        {error, Reason} ->
            ?LOG(error, "[Mnesia] do_check_user error: ~p~n", [Reason]),
            [];
        Re -> Re
    end.

%% @doc Remove user
-spec(remove_user(binary()) -> ok | {error, any()}).
remove_user(Login) ->
    ret(mnesia:transaction(fun mnesia:delete/1, [{?TABLE, Login}])).

%% @doc All logins
-spec(all_users() -> list()).
all_users() -> mnesia:dirty_all_keys(?TABLE).

%%--------------------------------------------------------------------
%% Acl API
%%--------------------------------------------------------------------

%% @doc Add Acls
-spec(add_acl(binary(), binary(), binary(), atom()) -> ok | {error, any()}).
add_acl(Login, Topic, Action, Allow) ->
    Acls = #emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow},
    ret(mnesia:transaction(fun mnesia:write/1, [Acls])).

%% @doc Lookup acl by login
-spec(lookup_acl(binary()) -> list()).
lookup_acl(undefined) -> [];
lookup_acl(Login) ->
    case mnesia:dirty_read(emqx_acl, Login) of
        {error, Reason} ->
            ?LOG(error, "[Mnesia] do_check_acl error: ~p~n", [Reason]),
            [];
        Re -> Re
    end.

%% @doc Remove acl
-spec(remove_acl(binary(), binary()) -> ok | {error, any()}).
remove_acl(Login, Topic) ->
    [ ok = mnesia:dirty_delete_object(emqx_acl, #emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow})  || [Action, Allow] <- ets:select(emqx_acl, [{{emqx_acl, Login, Topic,'$1','$2'}, [], ['$$']}])],
    ok.


%% @doc All logins
-spec(all_acls() -> list()).
all_acls() -> mnesia:dirty_all_keys(emqx_acl).


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.

encrypted_data(Password) ->
    HashType = application:get_env(emqx_auth_mnesia, hash_type, sha256),
    emqx_passwd:hash(HashType, Password).

%%--------------------------------------------------------------------
%% Auth APIs
%%--------------------------------------------------------------------

%% User
auth_cli(["add", Login, Password, IsSuperuser]) ->
    case add_user(iolist_to_binary(Login), iolist_to_binary(Password), IsSuperuser) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_cli(["update", Login, NewPassword, IsSuperuser]) ->
    case update_user(iolist_to_binary(Login), iolist_to_binary(NewPassword), IsSuperuser) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_cli(["del", Login]) ->
    case  remove_user(iolist_to_binary(Login)) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_cli(["show", P]) ->
    [emqx_ctl:print("User(login = ~p is_super = ~p)~n", [Login, IsSuperuser])
     || {_, Login, _Password, IsSuperuser} <- lookup_user(iolist_to_binary(P))];

auth_cli(["list"]) ->
    [emqx_ctl:print("User(login = ~p)~n",[E])
     || E <- all_users()];

auth_cli(_) ->
    emqx_ctl:usage([{"mqtt-user add <Login> <Password> <IsSuper>", "Add user"},
                    {"mqtt-user update <Login> <NewPassword> <IsSuper>", "Update user"},
                    {"mqtt-user delete <Login>", "Delete user"},
                    {"mqtt-user show <Login>", "Lookup user detail"},
                    {"mqtt-user list", "List all users"}]).

%% Acl
acl_cli(["add", Login, Topic, Action, Allow]) ->
    case add_acl(iolist_to_binary(Login), iolist_to_binary(Topic), iolist_to_binary(Action), Allow) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

acl_cli(["del", Login, Topic])->
    case remove_acl(iolist_to_binary(Login), iolist_to_binary(Topic)) of
         ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

acl_cli(["show", P]) ->
    [emqx_ctl:print("Acl(login = ~p topic = ~p action = ~p allow = ~p)~n",[Login, Topic, Action, Allow])
     || {_, Login, Topic, Action, Allow} <- lookup_acl(iolist_to_binary(P)) ];

acl_cli(["list"]) ->
    [emqx_ctl:print("Acl(login = ~p)~n",[E])
     || E <- all_acls() ];

acl_cli(_) ->
    emqx_ctl:usage([{"mqtt-acl add <Login> <Topic> <Action> <Allow>", "Add acl"},
                    {"mqtt-acl show <Login>", "Lookup acl detail"},
                    {"mqtt-acl del <Login>", "Delete acl"},
                    {"mqtt-acl list","List all acls"}]).
