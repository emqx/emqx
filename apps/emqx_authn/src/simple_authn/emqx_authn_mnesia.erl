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
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ namespace/0
        , roots/0
        , fields/1
        ]).

-export([ refs/0
        , create/2
        , update/2
        , authenticate/2
        , destroy/1
        ]).

-export([ import_users/2
        , add_user/2
        , delete_user/2
        , update_user/3
        , lookup_user/2
        , list_users/2
        ]).

-export([format_user_info/1]).

-type user_id_type() :: clientid | username.
-type user_group() :: binary().
-type user_id() :: binary().

-record(user_info,
        { user_id :: {user_group(), user_id()}
        , password_hash :: binary()
        , salt :: binary()
        , is_superuser :: boolean()
        }).

-reflect_type([user_id_type/0]).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-define(TAB, ?MODULE).
-define(FORMAT_FUN, {?MODULE, format_user_info}).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = mria:create_table(?TAB, [
                {rlog_shard, ?AUTH_SHARD},
                {storage, disc_copies},
                {record_name, user_info},
                {attributes, record_info(fields, user_info)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-builtin_db".

roots() -> [?CONF_NS].

fields(?CONF_NS) ->
    [ {mechanism, emqx_authn_schema:mechanism('password-based')}
    , {backend, emqx_authn_schema:backend('built-in-database')}
    , {user_id_type,            fun user_id_type/1}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    ] ++ emqx_authn_schema:common_fields();

fields(bcrypt) ->
    [ {name, {enum, [bcrypt]}}
    , {salt_rounds, fun salt_rounds/1}
    ];

fields(other_algorithms) ->
    [ {name, {enum, [plain, md5, sha, sha256, sha512]}}
    ].

user_id_type(type) -> user_id_type();
user_id_type(default) -> <<"username">>;
user_id_type(_) -> undefined.

password_hash_algorithm(type) -> hoconsc:union([hoconsc:ref(?MODULE, bcrypt),
                                                hoconsc:ref(?MODULE, other_algorithms)]);
password_hash_algorithm(default) -> #{<<"name">> => sha256};
password_hash_algorithm(_) -> undefined.

salt_rounds(type) -> integer();
salt_rounds(default) -> 10;
salt_rounds(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
   [hoconsc:ref(?MODULE, ?CONF_NS)].

create(AuthenticatorID,
       #{user_id_type := Type,
         password_hash_algorithm := #{name := bcrypt,
                                      salt_rounds := SaltRounds}}) ->
    ok = emqx_authn_utils:ensure_apps_started(bcrypt),
    State = #{user_group => AuthenticatorID,
              user_id_type => Type,
              password_hash_algorithm => bcrypt,
              salt_rounds => SaltRounds},
    {ok, State};

create(AuthenticatorID,
       #{user_id_type := Type,
         password_hash_algorithm := #{name := Name}}) ->
    ok = emqx_authn_utils:ensure_apps_started(Name),
    State = #{user_group => AuthenticatorID,
              user_id_type => Type,
              password_hash_algorithm => Name},
    {ok, State}.

update(Config, #{user_group := ID}) ->
    create(ID, Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := Password} = Credential,
             #{user_group := UserGroup,
               user_id_type := Type,
               password_hash_algorithm := Algorithm}) ->
    UserID = get_user_identity(Credential, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [] ->
            ignore;
        [#user_info{password_hash = PasswordHash, salt = Salt0, is_superuser = IsSuperuser}] ->
            Salt = case Algorithm of
                       bcrypt -> PasswordHash;
                       _ -> Salt0
                   end,
            case PasswordHash =:= hash(Algorithm, Password, Salt) of
                true -> {ok, #{is_superuser => IsSuperuser}};
                false -> {error, bad_username_or_password}
            end
    end.

destroy(#{user_group := UserGroup}) ->
    trans(
      fun() ->
              ok = lists:foreach(
                     fun(User) ->
                             mnesia:delete_object(?TAB, User, write)
                     end,
                     mnesia:select(?TAB, group_match_spec(UserGroup), write))
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

add_user(#{user_id := UserID,
           password := Password} = UserInfo,
         #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {PasswordHash, Salt} = hash(Password, State),
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
                    {ok, #{user_id => UserID, is_superuser => IsSuperuser}};
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

update_user(UserID, UserInfo,
            #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [#user_info{ password_hash = PasswordHash
                           , salt = Salt
                           , is_superuser = IsSuperuser}] ->
                    NSuperuser = maps:get(is_superuser, UserInfo, IsSuperuser),
                    {NPasswordHash, NSalt} = case maps:get(password, UserInfo, undefined) of
                                                 undefined ->
                                                     {PasswordHash, Salt};
                                                 Password ->
                                                     hash(Password, State)
                                             end,
                    insert_user(UserGroup, UserID, NPasswordHash, NSalt, NSuperuser),
                    {ok, #{user_id => UserID, is_superuser => NSuperuser}}
            end
        end).

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(PageParams, #{user_group := UserGroup}) ->
    {ok, emqx_mgmt_api:paginate(?TAB, group_match_spec(UserGroup), PageParams, ?FORMAT_FUN)}.

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
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
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
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
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
get_user_info_by_seq([Salt | More1], [<<"salt">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{salt => Salt});
get_user_info_by_seq([<<"true">> | More1], [<<"is_superuser">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{is_superuser => true});
get_user_info_by_seq([<<"false">> | More1], [<<"is_superuser">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{is_superuser => false});
get_user_info_by_seq(_, _, _) ->
    {error, bad_format}.

gen_salt(#{password_hash_algorithm := plain}) ->
    <<>>;
gen_salt(#{password_hash_algorithm := bcrypt,
           salt_rounds := Rounds}) ->
    {ok, Salt} = bcrypt:gen_salt(Rounds),
    Salt;
gen_salt(_) ->
    emqx_authn_utils:gen_salt().

hash(bcrypt, Password, Salt) ->
    {ok, Hash} = bcrypt:hashpw(Password, Salt),
    list_to_binary(Hash);
hash(Algorithm, Password, Salt) ->
    emqx_passwd:hash(Algorithm, <<Salt/binary, Password/binary>>).

hash(Password, #{password_hash_algorithm := Algorithm} = State) ->
    Salt = gen_salt(State),
    PasswordHash = hash(Algorithm, Password, Salt),
    {PasswordHash, Salt}.

insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser) ->
     UserInfo = #user_info{user_id = {UserGroup, UserID},
                           password_hash = PasswordHash,
                           salt = Salt,
                           is_superuser = IsSuperuser},
    mnesia:write(?TAB, UserInfo, write).

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
    case mria:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

to_binary(B) when is_binary(B) ->
    B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).

format_user_info(#user_info{user_id = {_, UserID}, is_superuser = IsSuperuser}) ->
    #{user_id => UserID, is_superuser => IsSuperuser}.

group_match_spec(UserGroup) ->
    ets:fun2ms(
      fun(#user_info{user_id = {Group, _}} = User) when Group =:= UserGroup ->
              User
      end).
