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

-module(emqx_authn_mnesia).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0, fields/1 ]).

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

-type user_id_type() :: clientid | username.

-type user_group() :: {chain_id(), service_name()}.
-type user_id() :: binary().

-record(user_info,
        { user_id :: {user_group(), user_id()}
        , password_hash :: binary()
        , salt :: binary()
        }).

-reflect_type([ user_id_type/0 ]).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(TAB, mnesia_basic_auth).

-rlog_shard({?AUTH_SHARD, ?TAB}).
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

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

structs() -> [config].

fields(config) ->
    [ {user_id_type, fun user_id_type/1}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    ];

fields(bcrypt) ->
    [ {name, {enum, [bcrypt]}}
    , {salt_rounds, fun salt_rounds/1}
    ];

fields(other_algorithms) ->
    [ {name, {enum, [plain, md5, sha, sha256, sha512]}}
    ].

user_id_type(type) -> user_id_type();
user_id_type(default) -> clientid;
user_id_type(_) -> undefined.

password_hash_algorithm(type) -> {union, [hoconsc:ref(bcrypt), hoconsc:ref(other_algorithms)]};
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

salt_rounds(type) -> integer();
salt_rounds(default) -> 10;
salt_rounds(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(ChainID, ServiceName, #{user_id_type := Type,
                               password_hash_algorithm := Algorithm,
                               salt_rounds := SaltRounds}) ->
    Algorithm =:= bcrypt andalso ({ok, _} = application:ensure_all_started(bcrypt)),
    State = #{user_group => {ChainID, ServiceName},
              user_id_type => Type,
              password_hash_algorithm => Algorithm,
              salt_rounds => SaltRounds},
    {ok, State}.

update(ChainID, ServiceName, Config, _State) ->
    create(ChainID, ServiceName, Config).

authenticate(ClientInfo = #{password := Password},
             #{user_group := UserGroup,
               user_id_type := Type,
               password_hash_algorithm := Algorithm}) ->
    UserID = get_user_identity(ClientInfo, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [] ->
            ignore;
        [#user_info{password_hash = PasswordHash, salt = Salt0}] ->
            Salt = case Algorithm of
                       bcrypt -> PasswordHash;
                       _ -> Salt0
                   end,
            case PasswordHash =:= hash(Algorithm, Password, Salt) of
                true -> ok;
                false -> {stop, bad_password}
            end
    end.

destroy(#{user_group := UserGroup}) ->
    trans(
        fun() ->
            MatchSpec = [{{user_info, {UserGroup, '_'}, '_', '_'}, [], ['$_']}],
            ok = lists:foreach(fun delete_user2/1, mnesia:select(?TAB, MatchSpec, write))
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
         #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    add(UserID, Password, State),
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
            #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    add(UserID, Password, State),
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
                    _ = file:close(File),
                    Result;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

import(_UserGroup, []) ->
    ok;
import(UserGroup, [#{<<"user_id">> := UserID,
                     <<"password_hash">> := PasswordHash} = UserInfo | More])
  when is_binary(UserID) andalso is_binary(PasswordHash) ->
    Salt = maps:get(<<"salt">>, UserInfo, <<>>),
    insert_user(UserGroup, UserID, PasswordHash, Salt),
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
                       password_hash := PasswordHash} = UserInfo} ->
                    Salt = maps:get(salt, UserInfo, <<>>),
                    insert_user(UserGroup, UserID, PasswordHash, Salt),
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

get_user_info_by_seq([], [], #{user_id := _, password_hash := _, salt := _} = Acc) ->
    {ok, Acc};
get_user_info_by_seq([], [], #{user_id := _, password_hash := _} = Acc) ->
    {ok, Acc};
get_user_info_by_seq(_, [], _) ->
    {error, bad_format};
get_user_info_by_seq([UserID | More1], [<<"user_id">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{user_id => UserID});
get_user_info_by_seq([PasswordHash | More1], [<<"password_hash">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{password_hash => PasswordHash});
get_user_info_by_seq([Salt | More1], [<<"salt">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{salt => Salt});
get_user_info_by_seq(_, _, _) ->
    {error, bad_format}.

-compile({inline, [add/3]}).
add(UserID, Password, #{user_group := UserGroup,
                        password_hash_algorithm := Algorithm} = State) ->
    Salt = gen_salt(State),
    PasswordHash = hash(Algorithm, Password, Salt),
    case Algorithm of
        bcrypt -> insert_user(UserGroup, UserID, PasswordHash);
        _ -> insert_user(UserGroup, UserID, PasswordHash, Salt)
    end.

gen_salt(#{password_hash_algorithm := plain}) ->
    <<>>;
gen_salt(#{password_hash_algorithm := bcrypt,
           salt_rounds := Rounds}) ->
    {ok, Salt} = bcrypt:gen_salt(Rounds),
    Salt;
gen_salt(_) ->
    <<X:128/big-unsigned-integer>> = crypto:strong_rand_bytes(16),
    iolist_to_binary(io_lib:format("~32.16.0b", [X])).

hash(bcrypt, Password, Salt) ->
    {ok, Hash} = bcrypt:hashpw(Password, Salt),
    list_to_binary(Hash);
hash(Algorithm, Password, Salt) ->
    emqx_passwd:hash(Algorithm, <<Salt/binary, Password/binary>>).

insert_user(UserGroup, UserID, PasswordHash) ->
    insert_user(UserGroup, UserID, PasswordHash, <<>>).

insert_user(UserGroup, UserID, PasswordHash, Salt) ->
     Credential = #user_info{user_id = {UserGroup, UserID},
                             password_hash = PasswordHash,
                             salt = Salt},
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
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.


to_binary(B) when is_binary(B) ->
    B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).
