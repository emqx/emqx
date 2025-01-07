%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_scram_mnesia).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(emqx_authn_provider).
-behaviour(emqx_db_backup).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    add_user/2,
    delete_user/2,
    update_user/3,
    lookup_user/2,
    list_users/2
]).

-export([
    qs2ms/2,
    run_fuzzy_filter/2,
    format_user_info/1,
    group_match_spec/1
]).

-export([backup_tables/0]).

%% Internal exports (RPC)
-export([
    do_destroy/1,
    do_add_user/1,
    do_delete_user/2,
    do_update_user/3
]).

-define(TAB, ?MODULE).
-define(AUTHN_QSCHEMA, [
    {<<"like_user_id">>, binary},
    {<<"user_group">>, binary},
    {<<"is_superuser">>, atom}
]).

-type user_group() :: binary().

-export([init_tables/0]).

-record(user_info, {
    user_id,
    stored_key,
    server_key,
    salt,
    is_superuser
}).

-reflect_type([user_group/0]).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(?TAB, [
        {rlog_shard, ?AUTHN_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, user_info},
        {attributes, record_info(fields, user_info)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?TAB].

-spec init_tables() -> ok.
init_tables() ->
    mria:wait_for_tables(create_tables()).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

backup_tables() -> {<<"builtin_authn">>, [?TAB]}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(
    AuthenticatorID,
    #{
        algorithm := Algorithm,
        iteration_count := IterationCount
    }
) ->
    State = #{
        user_group => AuthenticatorID,
        algorithm => Algorithm,
        iteration_count => IterationCount
    },
    {ok, State}.

update(Config, #{user_group := ID}) ->
    create(ID, Config).

authenticate(
    #{
        auth_method := AuthMethod,
        auth_data := AuthData,
        auth_cache := AuthCache
    },
    State
) ->
    RetrieveFun = fun(Username) ->
        retrieve(Username, State)
    end,
    OnErrFun = fun(Msg, Reason) ->
        ?TRACE_AUTHN_PROVIDER(Msg, #{
            reason => Reason
        })
    end,
    emqx_utils_scram:authenticate(
        AuthMethod, AuthData, AuthCache, State, RetrieveFun, OnErrFun, [is_superuser]
    );
authenticate(_Credential, _State) ->
    ignore.

destroy(#{user_group := UserGroup}) ->
    trans(fun ?MODULE:do_destroy/1, [UserGroup]).

do_destroy(UserGroup) ->
    MatchSpec = group_match_spec(UserGroup),
    ok = lists:foreach(
        fun(UserInfo) ->
            mnesia:delete_object(?TAB, UserInfo, write)
        end,
        mnesia:select(?TAB, MatchSpec, write)
    ).

add_user(UserInfo, State) ->
    UserInfoRecord = user_info_record(UserInfo, State),
    trans(fun ?MODULE:do_add_user/1, [UserInfoRecord]).

do_add_user(
    #user_info{user_id = {UserID, _} = DBUserID, is_superuser = IsSuperuser} = UserInfoRecord
) ->
    case mnesia:read(?TAB, DBUserID, write) of
        [] ->
            mnesia:write(?TAB, UserInfoRecord, write),
            {ok, #{user_id => UserID, is_superuser => IsSuperuser}};
        [_] ->
            {error, already_exist}
    end.

delete_user(UserID, State) ->
    trans(fun ?MODULE:do_delete_user/2, [UserID, State]).

do_delete_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:read(?TAB, {UserGroup, UserID}, write) of
        [] ->
            {error, not_found};
        [_] ->
            mnesia:delete(?TAB, {UserGroup, UserID}, write)
    end.

update_user(UserID, User, State) ->
    FieldsToUpdate = fields_to_update(
        User,
        [
            keys_and_salt,
            is_superuser
        ],
        State
    ),
    trans(fun ?MODULE:do_update_user/3, [UserID, FieldsToUpdate, State]).

do_update_user(
    UserID,
    FieldsToUpdate,
    #{user_group := UserGroup} = _State
) ->
    case mnesia:read(?TAB, {UserGroup, UserID}, write) of
        [] ->
            {error, not_found};
        [#user_info{} = UserInfo0] ->
            UserInfo1 = update_user_record(UserInfo0, FieldsToUpdate),
            mnesia:write(?TAB, UserInfo1, write),
            {ok, format_user_info(UserInfo1)}
    end.

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(QueryString, #{user_group := UserGroup}) ->
    NQueryString = QueryString#{<<"user_group">> => UserGroup},
    emqx_mgmt_api:node_query(
        node(),
        ?TAB,
        NQueryString,
        ?AUTHN_QSCHEMA,
        fun ?MODULE:qs2ms/2,
        fun ?MODULE:format_user_info/1
    ).

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {QString, Fuzzy}) ->
    #{
        match_spec => ms_from_qstring(QString),
        fuzzy_fun => fuzzy_filter_fun(Fuzzy)
    }.

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, UserID}},
    [{user_id, like, UserIDSubStr} | Fuzzy]
) ->
    binary:match(UserID, UserIDSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

user_info_record(
    #{
        user_id := UserID,
        password := Password
    } = UserInfo,
    #{user_group := UserGroup} = State
) ->
    IsSuperuser = maps:get(is_superuser, UserInfo, false),
    user_info_record(UserGroup, UserID, Password, IsSuperuser, State).

user_info_record(UserGroup, UserID, Password, IsSuperuser, State) ->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, State),
    #user_info{
        user_id = {UserGroup, UserID},
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser
    }.

fields_to_update(
    #{password := Password} = UserInfo,
    [keys_and_salt | Rest],
    State
) ->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, State),
    [
        {keys_and_salt, {StoredKey, ServerKey, Salt}}
        | fields_to_update(UserInfo, Rest, State)
    ];
fields_to_update(#{is_superuser := IsSuperuser} = UserInfo, [is_superuser | Rest], State) ->
    [{is_superuser, IsSuperuser} | fields_to_update(UserInfo, Rest, State)];
fields_to_update(UserInfo, [_ | Rest], State) ->
    fields_to_update(UserInfo, Rest, State);
fields_to_update(_UserInfo, [], _State) ->
    [].

update_user_record(UserInfoRecord, []) ->
    UserInfoRecord;
update_user_record(UserInfoRecord, [{keys_and_salt, {StoredKey, ServerKey, Salt}} | Rest]) ->
    update_user_record(
        UserInfoRecord#user_info{
            stored_key = StoredKey,
            server_key = ServerKey,
            salt = Salt
        },
        Rest
    );
update_user_record(UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{is_superuser = IsSuperuser}, Rest).

retrieve(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [
            #user_info{
                stored_key = StoredKey,
                server_key = ServerKey,
                salt = Salt,
                is_superuser = IsSuperuser
            }
        ] ->
            {ok, #{
                stored_key => StoredKey,
                server_key => ServerKey,
                salt => Salt,
                is_superuser => IsSuperuser
            }};
        [] ->
            {error, not_found}
    end.

%% TODO: Move to emqx_authn_utils.erl
trans(Fun, Args) ->
    case mria:transaction(?AUTHN_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, {function_clause, Stack}} -> erlang:raise(error, function_clause, Stack);
        {aborted, Reason} -> {error, Reason}
    end.

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
    ets:fun2ms(
        fun(#user_info{user_id = {Group, _}} = User) when Group =:= UserGroup ->
            User
        end
    ).

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
