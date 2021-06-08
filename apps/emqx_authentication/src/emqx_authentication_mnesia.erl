%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , update/4
        , authenticate/2
        , destroy/1
        ]).

-export([ import_users/2
        , add_user/2
        , delete_user/2
        , update_user/3
        , lookup_user/2
        , list_users/1
        ]).

-service_type(#{
    name => mnesia,
    params_spec => #{
        user_id_type => #{
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

-record(user_info,
        { user_id :: {user_group(), user_id()}
        , password_hash :: binary()
        }).

-type(user_group() :: {chain_id(), service_name()}).
-type(user_id() :: binary()).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(TAB, mnesia_basic_auth).

%% TODO: Support salt

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {disc_copies, [node()]},
                {record_name, user_info},
                {attributes, record_info(fields, user_info)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

create(ChainID, ServiceName, #{<<"user_id_type">> := Type,
                               <<"password_hash_algorithm">> := Algorithm}) ->
    State = #{user_group => {ChainID, ServiceName},
              user_id_type => binary_to_atom(Type, utf8),
              password_hash_algorithm => binary_to_atom(Algorithm, utf8)},
    {ok, State}.

update(ChainID, ServiceName, Params, _State) ->
    create(ChainID, ServiceName, Params).

authenticate(ClientInfo = #{password := Password},
             #{user_group := UserGroup,
               user_id_type := Type,
               password_hash_algorithm := Algorithm}) ->
    UserID = get_user_identity(ClientInfo, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [] ->
            ignore;
        [#user_info{password_hash = Hash}] ->
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
            MatchSpec = [{#user_info{user_id = {UserGroup, '_'}, _ = '_'}, [], ['$_']}],
            lists:foreach(fun delete_user2/1, mnesia:select(?TAB, MatchSpec, write))
        end).

import_users(Filename0, State) ->
    Filename = to_binary(Filename0),
    case filename:extension(Filename) of
        <<".json">> ->
            import_users_from_json(Filename, State);
        <<".csv">> ->
            import_users_from_csv(Filename, State);
        <<>> ->
            {error, unknown_file_format};
        Extension ->
            {error, {unsupported_file_format, Extension}}
    end.

add_user(#{<<"user_id">> := UserID,
           <<"password">> := Password},
         #{user_group := UserGroup,
           password_hash_algorithm := Algorithm}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    add(UserGroup, UserID, Password, Algorithm),
                    {ok, #{user_id => UserID}};
                [_] ->
                    {error, already_exist}
            end
        end).

delete_user(UserID, #{user_group := UserGroup}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    mnesia:delete(?TAB, {UserGroup, UserID}, write)
            end
        end).

update_user(UserID, #{<<"password">> := Password},
            #{user_group := UserGroup,
              password_hash_algorithm := Algorithm}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    add(UserGroup, UserID, Password, Algorithm),
                    {ok, #{user_id => UserID}}
            end
        end).

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [#user_info{user_id = {_, UserID}}] ->
            {ok, #{user_id => UserID}};
        [] ->
            {error, not_found}
    end.

list_users(#{user_group := UserGroup}) ->
    Users = [#{user_id => UserID} || #user_info{user_id = {UserGroup0, UserID}} <- ets:tab2list(?TAB), UserGroup0 =:= UserGroup],
    {ok, Users}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Example: data/user-credentials.json
import_users_from_json(Filename, #{user_group := UserGroup}) ->
    case file:read_file(Filename) of
        {ok, Bin} ->
            case emqx_json:safe_decode(Bin, [return_maps]) of
                {ok, List} ->
                    trans(fun import/2, [UserGroup, List]);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Example: data/user-credentials.csv
import_users_from_csv(Filename, #{user_group := UserGroup}) ->
    case file:open(Filename, [read, binary]) of
        {ok, File} ->
            case get_csv_header(File) of
                {ok, Seq} ->
                    Result = trans(fun import/3, [UserGroup, File, Seq]),
                    file:close(File),
                    Result;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

import(_UserGroup, []) ->
    ok;
import(UserGroup, [#{<<"user_id">> := UserID, <<"password_hash">> := PasswordHash} | More])
  when is_binary(UserID) andalso is_binary(PasswordHash) ->
    import_user(UserGroup, UserID, PasswordHash),
    import(UserGroup, More);
import(_UserGroup, [_ | _More]) ->
    {error, bad_format}.

%% Importing 5w users needs 1.7 seconds 
import(UserGroup, File, Seq) ->
    case file:read_line(File) of
        {ok, Line} ->
            Fields = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            case get_user_info_by_seq(Fields, Seq) of
                {ok, #{user_id := UserID,
                       password_hash := PasswordHash}} ->
                    import_user(UserGroup, UserID, PasswordHash),
                    import(UserGroup, File, Seq);
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

get_csv_header(File) ->
    case file:read_line(File) of
        {ok, Line} ->
            Seq = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            {ok, Seq};
        eof ->
            {error, empty_file};
        {error, Reason} ->
            {error, Reason}
    end.

get_user_info_by_seq(Fields, Seq) ->
    get_user_info_by_seq(Fields, Seq, #{}).

get_user_info_by_seq([], [], #{user_id := _, password_hash := _} = Acc) ->
    {ok, Acc};
get_user_info_by_seq(_, [], _) ->
    {error, bad_format};
get_user_info_by_seq([UserID | More1], [<<"user_id">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{user_id => UserID});
get_user_info_by_seq([PasswordHash | More1], [<<"password_hash">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{password_hash => PasswordHash});
get_user_info_by_seq(_, _, _) ->
    {error, bad_format}.

-compile({inline, [add/4]}).
add(UserGroup, UserID, Password, Algorithm) ->
    Credential = #user_info{user_id = {UserGroup, UserID},
                            password_hash = emqx_passwd:hash(Algorithm, Password)},
    mnesia:write(?TAB, Credential, write).

import_user(UserGroup, UserID, PasswordHash) ->
    Credential = #user_info{user_id = {UserGroup, UserID},
                            password_hash = PasswordHash},
    mnesia:write(?TAB, Credential, write).

delete_user2(UserInfo) ->
    mnesia:delete_object(?TAB, UserInfo, write).

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


to_binary(B) when is_binary(B) ->
    B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).
