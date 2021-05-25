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

-module(emqx_authentication_mnesia).

-include("emqx_authentication.hrl").

-export([ create/3
        , authenticate/2
        , destroy/1
        ]).

-export([ import_user_credentials/3
        , add_user_credential/2
        , delete_user_credential/2
        , update_user_credential/2
        , lookup_user_credential/2
        ]).

-service_type(#{
    name => mnesia,
    params_spec => #{
        user_identity_type => #{
            order => 1,
            type => string,
            required => true,
            enum => [<<"username">>, <<"clientid">>, <<"ip">>, <<"common name">>, <<"issuer">>],
            default => <<"username">>
        },
        password_hash_algorithm => #{
            order => 2,
            type => string,
            required => true,
            enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>],
            default => <<"sha256">>
        }
    }
}).

-record(user_credential,
        { user_identity :: {user_group(), user_identity()}
        , password_hash :: binary()
        }).

-type(user_group() :: {chain_id(), service_name()}).
-type(user_identity() :: binary()).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(TAB, mnesia_basic_auth).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {disc_copies, [node()]},
                {record_name, user_credential},
                {attributes, record_info(fields, user_credential)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

create(ChainID, ServiceName, #{<<"user_identity_type">> := Type,
                               <<"password_hash_algorithm">> := Algorithm}) ->
    State = #{user_group => {ChainID, ServiceName},
              user_identity_type => binary_to_atom(Type, utf8),
              password_hash_algorithm => binary_to_atom(Algorithm, utf8)},
    {ok, State}.

authenticate(ClientInfo = #{password := Password},
             #{user_group := UserGroup,
               user_identity_type := Type,
               password_hash_algorithm := Algorithm}) ->
    UserIdentity = get_user_identity(ClientInfo, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserIdentity}) of
        [] ->
            ignore;
        [#user_credential{password_hash = Hash}] ->
            case Hash =:= emqx_passwd:hash(Algorithm, Password) of
                true ->
                    ok;
                false ->
                    {stop, bad_password}
            end
    end.

destroy(#{user_group := UserGroup}) ->
    trans(
        fun() ->
            MatchSpec = [{#user_credential{user_identity = {UserGroup, '_'}, _ = '_'}, [], ['$_']}],
            lists:foreach(fun delete_user_credential/1, mnesia:select(?TAB, MatchSpec, write))
        end).

%% Example:
%% {
%%     "myuser1":"mypassword1",
%%     "myuser2":"mypassword2"
%% }
import_user_credentials(Filename, json, 
                        #{user_group := UserGroup,
                          password_hash_algorithm := Algorithm}) ->
    case file:read_file(Filename) of
        {ok, Bin} ->
            case emqx_json:safe_decode(Bin) of
                {ok, List} ->
                    import(UserGroup, List, Algorithm);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
%% Example:
%% myuser1,mypassword1
%% myuser2,mypassword2
import_user_credentials(Filename, csv,
                        #{user_group := UserGroup,
                          password_hash_algorithm := Algorithm}) ->
    case file:open(Filename, [read, binary]) of
        {ok, File} ->
            Result = import(UserGroup, File, Algorithm),
            file:close(File),
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

add_user_credential(#{user_identity := UserIdentity, password := Password},
                    #{user_group := UserGroup,
                      password_hash_algorithm := Algorithm}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserIdentity}, write) of
                [] ->
                    add(UserGroup, UserIdentity, Password, Algorithm);
                [_] ->
                    {error, already_exist}
            end
        end).

delete_user_credential(UserIdentity, #{user_group := UserGroup}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserIdentity}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    mnesia:delete(?TAB, {UserGroup, UserIdentity}, write)
            end
        end).

update_user_credential(#{user_identity := UserIdentity, password := Password},
                       #{user_group := UserGroup,
                         password_hash_algorithm := Algorithm}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserIdentity}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    add(UserGroup, UserIdentity, Password, Algorithm)
            end
        end).

lookup_user_credential(UserIdentity, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserIdentity}) of
        [#user_credential{user_identity = {_, UserIdentity},
                          password_hash = PassHash}] ->
            {ok, #{user_identity => UserIdentity,
                   password_hash => PassHash}};
        [] -> {error, not_found}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

import(UserGroup, ListOrFile, Algorithm) ->
    trans(fun do_import/3, [UserGroup, ListOrFile, Algorithm]).

do_import(_UserGroup, [], _Algorithm) ->
    ok;
do_import(UserGroup, [{UserIdentity, Password} | More], Algorithm)
  when is_binary(UserIdentity) andalso is_binary(Password) ->
    add(UserGroup, UserIdentity, Password, Algorithm),
    do_import(UserGroup, More, Algorithm);
do_import(_UserGroup, [_ | _More], _Algorithm) ->
    {error, bad_format};

%% Importing 5w credentials needs 1.7 seconds 
do_import(UserGroup, File, Algorithm)  ->
    case file:read_line(File) of
        {ok, Line} ->
            case binary:split(Line, [<<",">>, <<"\n">>], [global]) of
                [UserIdentity, Password, <<>>] ->
                    add(UserGroup, UserIdentity, Password, Algorithm),
                    do_import(UserGroup, File, Algorithm);
                [UserIdentity, Password] ->
                    add(UserGroup, UserIdentity, Password, Algorithm),
                    do_import(UserGroup, File, Algorithm);
                _ ->
                    {error, bad_format}
            end;
        eof ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-compile({inline, [add/4]}).
add(UserGroup, UserIdentity, Password, Algorithm) ->
    Credential = #user_credential{user_identity = {UserGroup, UserIdentity},
                                  password_hash = emqx_passwd:hash(Algorithm, Password)},
    mnesia:write(?TAB, Credential, write).

delete_user_credential(UserCredential) ->
    mnesia:delete_object(?TAB, UserCredential, write).

%% TODO: Support other type
get_user_identity(#{username := Username}, username) ->
    Username;
get_user_identity(#{clientid := ClientID}, clientid) ->
    ClientID;
get_user_identity(_, Type) ->
    {error, {bad_user_identity_type, Type}}.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.






