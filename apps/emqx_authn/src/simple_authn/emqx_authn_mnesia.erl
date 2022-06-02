%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    import_users/2,
    add_user/2,
    delete_user/2,
    update_user/3,
    lookup_user/2,
    list_users/2
]).

-export([
    query/4,
    format_user_info/1,
    group_match_spec/1
]).

-type user_group() :: binary().
-type user_id() :: binary().

-record(user_info, {
    user_id :: {user_group(), user_id()},
    password_hash :: binary(),
    salt :: binary(),
    is_superuser :: boolean()
}).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-define(TAB, ?MODULE).
-define(AUTHN_QSCHEMA, [
    {<<"like_user_id">>, binary},
    {<<"user_group">>, binary},
    {<<"is_superuser">>, atom}
]).
-define(QUERY_FUN, {?MODULE, query}).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec mnesia(boot | copy) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {rlog_shard, ?AUTH_SHARD},
        {storage, disc_copies},
        {record_name, user_info},
        {attributes, record_info(fields, user_info)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-builtin_db".

roots() -> [?CONF_NS].

fields(?CONF_NS) ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(built_in_database)},
        {user_id_type, fun user_id_type/1},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_rw/1}
    ] ++ emqx_authn_schema:common_fields().

desc(?CONF_NS) ->
    ?DESC(?CONF_NS);
desc(_) ->
    undefined.

user_id_type(type) -> hoconsc:enum([clientid, username]);
user_id_type(desc) -> ?DESC(?FUNCTION_NAME);
user_id_type(default) -> <<"username">>;
user_id_type(required) -> true;
user_id_type(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ?CONF_NS)].

create(_AuthenticatorID, Config) ->
    create(Config).

create(
    #{
        user_id_type := Type,
        password_hash_algorithm := Algorithm,
        user_group := UserGroup
    }
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    State = #{
        user_group => UserGroup,
        user_id_type => Type,
        password_hash_algorithm => Algorithm
    },
    {ok, State}.

update(Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    #{password := Password} = Credential,
    #{
        user_group := UserGroup,
        user_id_type := Type,
        password_hash_algorithm := Algorithm
    }
) ->
    UserID = get_user_identity(Credential, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [] ->
            ignore;
        [#user_info{password_hash = PasswordHash, salt = Salt, is_superuser = IsSuperuser}] ->
            case
                emqx_authn_password_hashing:check_password(
                    Algorithm, Salt, PasswordHash, Password
                )
            of
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
                mnesia:select(?TAB, group_match_spec(UserGroup), write)
            )
        end
    ).

import_users({Filename0, FileData}, State) ->
    Filename = to_binary(Filename0),
    case filename:extension(Filename) of
        <<".json">> ->
            import_users_from_json(FileData, State);
        <<".csv">> ->
            CSV = csv_data(FileData),
            import_users_from_csv(CSV, State);
        <<>> ->
            {error, unknown_file_format};
        Extension ->
            {error, {unsupported_file_format, Extension}}
    end.

add_user(
    #{
        user_id := UserID,
        password := Password
    } = UserInfo,
    #{
        user_group := UserGroup,
        password_hash_algorithm := Algorithm
    }
) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
                    {ok, #{user_id => UserID, is_superuser => IsSuperuser}};
                [_] ->
                    {error, already_exist}
            end
        end
    ).

delete_user(UserID, #{user_group := UserGroup}) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [_] ->
                    mnesia:delete(?TAB, {UserGroup, UserID}, write)
            end
        end
    ).

update_user(
    UserID,
    UserInfo,
    #{
        user_group := UserGroup,
        password_hash_algorithm := Algorithm
    }
) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [
                    #user_info{
                        password_hash = PasswordHash,
                        salt = Salt,
                        is_superuser = IsSuperuser
                    }
                ] ->
                    NSuperuser = maps:get(is_superuser, UserInfo, IsSuperuser),
                    {NPasswordHash, NSalt} =
                        case UserInfo of
                            #{password := Password} ->
                                emqx_authn_password_hashing:hash(
                                    Algorithm, Password
                                );
                            #{} ->
                                {PasswordHash, Salt}
                        end,
                    insert_user(UserGroup, UserID, NPasswordHash, NSalt, NSuperuser),
                    {ok, #{user_id => UserID, is_superuser => NSuperuser}}
            end
        end
    ).

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(QueryString, #{user_group := UserGroup}) ->
    NQueryString = QueryString#{<<"user_group">> => UserGroup},
    emqx_mgmt_api:node_query(node(), NQueryString, ?TAB, ?AUTHN_QSCHEMA, ?QUERY_FUN).

%%--------------------------------------------------------------------
%% Query Functions

query(Tab, {QString, []}, Continuation, Limit) ->
    Ms = ms_from_qstring(QString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_user_info/1
    );
query(Tab, {QString, FuzzyQString}, Continuation, Limit) ->
    Ms = ms_from_qstring(QString),
    FuzzyFilterFun = fuzzy_filter_fun(FuzzyQString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_user_info/1
    ).

%%--------------------------------------------------------------------
%% Match funcs

%% Fuzzy username funcs
fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, UserID}},
    [{user_id, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(UserID, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Example: data/user-credentials.json
import_users_from_json(Bin, #{user_group := UserGroup}) ->
    case emqx_json:safe_decode(Bin, [return_maps]) of
        {ok, List} ->
            trans(fun import/2, [UserGroup, List]);
        {error, Reason} ->
            {error, Reason}
    end.

%% Example: data/user-credentials.csv
import_users_from_csv(CSV, #{user_group := UserGroup}) ->
    case get_csv_header(CSV) of
        {ok, Seq, NewCSV} ->
            trans(fun import_csv/3, [UserGroup, NewCSV, Seq]);
        {error, Reason} ->
            {error, Reason}
    end.

import(_UserGroup, []) ->
    ok;
import(UserGroup, [
    #{
        <<"user_id">> := UserID,
        <<"password_hash">> := PasswordHash
    } = UserInfo
    | More
]) when
    is_binary(UserID) andalso is_binary(PasswordHash)
->
    Salt = maps:get(<<"salt">>, UserInfo, <<>>),
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
    import(UserGroup, More);
import(_UserGroup, [_ | _More]) ->
    {error, bad_format}.

%% Importing 5w users needs 1.7 seconds
import_csv(UserGroup, CSV, Seq) ->
    case csv_read_line(CSV) of
        {ok, Line, NewCSV} ->
            Fields = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            case get_user_info_by_seq(Fields, Seq) of
                {ok,
                    #{
                        user_id := UserID,
                        password_hash := PasswordHash
                    } = UserInfo} ->
                    Salt = maps:get(salt, UserInfo, <<>>),
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
                    import_csv(UserGroup, NewCSV, Seq);
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            ok
    end.

get_csv_header(CSV) ->
    case csv_read_line(CSV) of
        {ok, Line, NewCSV} ->
            Seq = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            {ok, Seq, NewCSV};
        eof ->
            {error, empty_file}
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

insert_user(UserGroup, UserID, PasswordHash, Salt, IsSuperuser) ->
    UserInfo = #user_info{
        user_id = {UserGroup, UserID},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    },
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

ms_from_qstring(QString) ->
    case lists:keytake(user_group, 1, QString) of
        {value, {user_group, '=:=', UserGroup}, QString2} ->
            group_match_spec(UserGroup, QString2);
        _ ->
            []
    end.

group_match_spec(UserGroup) ->
    group_match_spec(UserGroup, []).

group_match_spec(UserGroup, QString) ->
    case lists:keyfind(is_superuser, 1, QString) of
        false ->
            ets:fun2ms(fun(#user_info{user_id = {Group, _}} = User) when Group =:= UserGroup ->
                User
            end);
        {is_superuser, '=:=', Value} ->
            ets:fun2ms(fun(#user_info{user_id = {Group, _}, is_superuser = IsSuper} = User) when
                Group =:= UserGroup, IsSuper =:= Value
            ->
                User
            end)
    end.

csv_data(Data) ->
    Lines = binary:split(Data, [<<"\r">>, <<"\n">>], [global, trim_all]),
    {csv_data, Lines}.

csv_read_line({csv_data, [Line | Lines]}) ->
    {ok, Line, {csv_data, Lines}};
csv_read_line({csv_data, []}) ->
    eof.
