%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("stdlib/include/ms_transform.hrl").
-define(TABLE, emqx_user).
%% Auth APIs
-export([ add_user/2
        , force_add_user/2
        , add_default_user/3
        , update_user/2
        , remove_user/1
        , lookup_user/1
        , all_users/0
        , all_users/1
        ]).
%% Cli
-export([ auth_clientid_cli/1
        , auth_username_cli/1
        ]).

%% Helper
-export([comparing/2]).

%%--------------------------------------------------------------------
%% Auth APIs
%%--------------------------------------------------------------------

%% @doc Add User
-spec(add_user(tuple(), binary()) -> ok | {error, any()}).
add_user(Login, Password) ->
    User = #emqx_user{
              login = Login,
              password = encrypted_data(Password),
              created_at = erlang:system_time(millisecond)
             },
    ret(mnesia:transaction(fun insert_user/1, [User])).

insert_user(User = #emqx_user{login = Login}) ->
    case mnesia:read(?TABLE, Login) of
        []    -> mnesia:write(User);
        [_|_] -> mnesia:abort(existed)
    end.

-spec(add_default_user(clientid | username, binary(), binary()) -> ok | {error, any()}).
add_default_user(Type, Key, Password) ->
    Login = {Type, Key},
    case add_user(Login, Password) of
        ok -> ok;
        {error, existed} ->
            NewPwd = encrypted_data(Password),
            [#emqx_user{password = OldPwd}] = emqx_auth_mnesia_cli:lookup_user(Login),
            HashType = emqx_auth_mnesia:hash_type(),
            case emqx_auth_mnesia:match_password(NewPwd, HashType, [OldPwd])  of
                true -> ok;
                false ->
                    %% We can't force add default,
                    %% otherwise passwords that have been updated via HTTP API will be reset after reboot.
                    TypeCtl =
                        case Type of
                            clientid -> clientid;
                            username -> user
                        end,
                    ?LOG(warning,
                        "[Auth Mnesia] auth.client.x.~p=~s password in the emqx_auth_mnesia.conf\n"
                        "does not match the password in the database(mnesia).\n"
                        "1. If you have already changed the password via the HTTP API, this warning has no effect.\n"
                        "You can remove the `auth.client.x.~p=~s` from emqx_auth_mnesia.conf to resolve this warning.\n"
                        "2. If you just want to update the password by manually changing the configuration file,\n"
                        "you need to delete the old user and password using `emqx_ctl ~p delete ~s` first\n"
                        "the new password in emqx_auth_mnesia.conf can take effect after reboot.",
                        [Type, Key, Type, Key, TypeCtl, Key]),
                    ok
            end;
        Error -> Error
    end.

force_add_user(Login, Password) ->
    User = #emqx_user{
        login = Login,
        password = encrypted_data(Password),
        created_at = erlang:system_time(millisecond)
    },
    case ret(mnesia:transaction(fun insert_or_update_user/2, [Password, User])) of
        {ok, override} ->
            ?LOG(warning, "[Mnesia] (~p)'s password has be updated.", [Login]),
            ok;
        Other -> Other
    end.

insert_or_update_user(NewPwd, User = #emqx_user{login = Login}) ->
    case mnesia:read(?TABLE, Login) of
        []    -> mnesia:write(User);
        [#emqx_user{password = Pwd}] ->
            case emqx_auth_mnesia:match_password(NewPwd, emqx_auth_mnesia:hash_type(), [Pwd])  of
                true -> ok;
                false ->
                    ok = mnesia:write(User),
                    {ok, override}
            end
    end.


%% @doc Update User
-spec(update_user(tuple(), binary()) -> ok | {error, any()}).
update_user(Login, NewPassword) ->
    ret(mnesia:transaction(fun do_update_user/2, [Login, encrypted_data(NewPassword)])).

do_update_user(Login, NewPassword) ->
    case mnesia:read(?TABLE, Login) of
        [#emqx_user{} = User] ->
            mnesia:write(User#emqx_user{password = NewPassword});
        [] -> mnesia:abort(noexisted)
    end.

%% @doc Lookup user by login
-spec(lookup_user(tuple()) -> list()).
lookup_user(undefined) -> [];
lookup_user(Login) ->
    Re = mnesia:dirty_read(?TABLE, Login),
    lists:sort(fun comparing/2, Re).

%% @doc Remove user
-spec(remove_user(tuple()) -> ok | {error, any()}).
remove_user(Login) ->
    ret(mnesia:transaction(fun mnesia:delete/1, [{?TABLE, Login}])).

%% @doc All logins
-spec(all_users() -> list()).
all_users() -> mnesia:dirty_all_keys(?TABLE).

all_users(clientid) ->
    MatchSpec = ets:fun2ms(
                  fun({?TABLE, {clientid, Clientid}, Password, CreatedAt}) ->
                          {?TABLE, {clientid, Clientid}, Password, CreatedAt}
                  end),
    lists:sort(fun comparing/2, ets:select(?TABLE, MatchSpec));
all_users(username) ->
    MatchSpec = ets:fun2ms(
                  fun({?TABLE, {username, Username}, Password, CreatedAt}) ->
                          {?TABLE, {username, Username}, Password, CreatedAt}
                  end),
    lists:sort(fun comparing/2, ets:select(?TABLE, MatchSpec)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

comparing({?TABLE, _, _, CreatedAt1},
          {?TABLE, _, _, CreatedAt2}) ->
    CreatedAt1 >= CreatedAt2.

ret({atomic, Res}) -> Res;
ret({aborted, Error}) -> {error, Error}.

encrypted_data(Password) ->
    HashType = emqx_auth_mnesia:hash_type(),
    SaltBin = salt(),
    <<SaltBin/binary, (hash(Password, SaltBin, HashType))/binary>>.

hash(undefined, SaltBin, HashType) ->
    hash(<<>>, SaltBin, HashType);
hash(Password, SaltBin, HashType) ->
    emqx_passwd:hash(HashType, <<SaltBin/binary, Password/binary>>).

salt() ->
    {_AlgHandler, _AlgState} = rand:seed(exsplus, erlang:timestamp()),
    Salt = rand:uniform(16#ffffffff), <<Salt:32>>.

%%--------------------------------------------------------------------
%% Auth Clientid Cli
%%--------------------------------------------------------------------

auth_clientid_cli(["list"]) ->
    [emqx_ctl:print("~s~n", [ClientId])
     || {?TABLE, {clientid, ClientId}, _Password, _CreatedAt} <- all_users(clientid)
    ];

auth_clientid_cli(["add", ClientId, Password]) ->
    case add_user({clientid, iolist_to_binary(ClientId)}, iolist_to_binary(Password)) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_clientid_cli(["update", ClientId, NewPassword]) ->
    case update_user({clientid, iolist_to_binary(ClientId)}, iolist_to_binary(NewPassword)) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_clientid_cli(["del", ClientId]) ->
    auth_clientid_cli(["delete", ClientId]);

auth_clientid_cli(["delete", ClientId]) ->
    case  remove_user({clientid, iolist_to_binary(ClientId)}) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_clientid_cli(_) ->
    emqx_ctl:usage([{"clientid list", "List clientid auth rules"},
                    {"clientid add <Username> <Password>", "Add clientid auth rule"},
                    {"clientid update <Username> <NewPassword>", "Update clientid auth rule"},
                    {"clientid delete <Username>", "Delete clientid auth rule"}]).

%%--------------------------------------------------------------------
%% Auth Username Cli
%%--------------------------------------------------------------------

auth_username_cli(["list"]) ->
    [emqx_ctl:print("~s~n", [Username])
     || {?TABLE, {username, Username}, _Password, _CreatedAt} <- all_users(username)
    ];

auth_username_cli(["add", Username, Password]) ->
    case add_user({username, iolist_to_binary(Username)}, iolist_to_binary(Password)) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_username_cli(["update", Username, NewPassword]) ->
    case update_user({username, iolist_to_binary(Username)}, iolist_to_binary(NewPassword)) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;
auth_username_cli(["del", Username]) ->
    auth_username_cli(["delete", Username]);

auth_username_cli(["delete", Username]) ->
    case  remove_user({username, iolist_to_binary(Username)}) of
        ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

auth_username_cli(_) ->
    emqx_ctl:usage([{"user list", "List username auth rules"},
                    {"user add <Username> <Password>", "Add username auth rule"},
                    {"user update <Username> <NewPassword>", "Update username auth rule"},
                    {"user delete <Username>", "Delete username auth rule"}]).
