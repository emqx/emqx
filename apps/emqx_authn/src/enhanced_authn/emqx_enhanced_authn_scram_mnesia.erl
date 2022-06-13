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

-module(emqx_enhanced_authn_scram_mnesia).

-include("emqx_authn.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("typerefl/include/types.hrl").

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

-define(TAB, ?MODULE).
-define(AUTHN_QSCHEMA, [
    {<<"like_user_id">>, binary},
    {<<"user_group">>, binary},
    {<<"is_superuser">>, atom}
]).
-define(QUERY_FUN, {?MODULE, query}).

-type user_group() :: binary().

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

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

namespace() -> "authn-scram-builtin_db".

roots() -> [?CONF_NS].

fields(?CONF_NS) ->
    [
        {mechanism, emqx_authn_schema:mechanism(scram)},
        {backend, emqx_authn_schema:backend(built_in_database)},
        {algorithm, fun algorithm/1},
        {iteration_count, fun iteration_count/1}
    ] ++ emqx_authn_schema:common_fields().

desc(?CONF_NS) ->
    "Settings for Salted Challenge Response Authentication Mechanism\n"
    "(SCRAM) authentication.";
desc(_) ->
    undefined.

algorithm(type) -> hoconsc:enum([sha256, sha512]);
algorithm(desc) -> "Hashing algorithm.";
algorithm(default) -> sha256;
algorithm(_) -> undefined.

iteration_count(type) -> non_neg_integer();
iteration_count(desc) -> "Iteration count.";
iteration_count(default) -> 4096;
iteration_count(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ?CONF_NS)].

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
    case ensure_auth_method(AuthMethod, State) of
        true ->
            case AuthCache of
                #{next_step := client_final} ->
                    check_client_final_message(AuthData, AuthCache, State);
                _ ->
                    check_client_first_message(AuthData, AuthCache, State)
            end;
        false ->
            ignore
    end;
authenticate(_Credential, _State) ->
    ignore.

destroy(#{user_group := UserGroup}) ->
    MatchSpec = group_match_spec(UserGroup),
    trans(
        fun() ->
            ok = lists:foreach(
                fun(UserInfo) ->
                    mnesia:delete_object(?TAB, UserInfo, write)
                end,
                mnesia:select(?TAB, MatchSpec, write)
            )
        end
    ).

add_user(
    #{
        user_id := UserID,
        password := Password
    } = UserInfo,
    #{user_group := UserGroup} = State
) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    add_user(UserGroup, UserID, Password, IsSuperuser, State),
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
    User,
    #{user_group := UserGroup} = State
) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [#user_info{is_superuser = IsSuperuser} = UserInfo] ->
                    UserInfo1 = UserInfo#user_info{
                        is_superuser = maps:get(is_superuser, User, IsSuperuser)
                    },
                    UserInfo2 =
                        case maps:get(password, User, undefined) of
                            undefined ->
                                UserInfo1;
                            Password ->
                                {StoredKey, ServerKey, Salt} = esasl_scram:generate_authentication_info(
                                    Password, State
                                ),
                                UserInfo1#user_info{
                                    stored_key = StoredKey,
                                    server_key = ServerKey,
                                    salt = Salt
                                }
                        end,
                    mnesia:write(?TAB, UserInfo2, write),
                    {ok, format_user_info(UserInfo2)}
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
    [{user_id, like, UserIDSubStr} | Fuzzy]
) ->
    binary:match(UserID, UserIDSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

ensure_auth_method(<<"SCRAM-SHA-256">>, #{algorithm := sha256}) ->
    true;
ensure_auth_method(<<"SCRAM-SHA-512">>, #{algorithm := sha512}) ->
    true;
ensure_auth_method(_, _) ->
    false.

check_client_first_message(Bin, _Cache, #{iteration_count := IterationCount} = State) ->
    RetrieveFun = fun(Username) ->
        retrieve(Username, State)
    end,
    case
        esasl_scram:check_client_first_message(
            Bin,
            #{
                iteration_count => IterationCount,
                retrieve => RetrieveFun
            }
        )
    of
        {continue, ServerFirstMessage, Cache} ->
            {continue, ServerFirstMessage, Cache};
        ignore ->
            ignore;
        {error, _Reason} ->
            {error, not_authorized}
    end.

check_client_final_message(Bin, #{is_superuser := IsSuperuser} = Cache, #{algorithm := Alg}) ->
    case
        esasl_scram:check_client_final_message(
            Bin,
            Cache#{algorithm => Alg}
        )
    of
        {ok, ServerFinalMessage} ->
            {ok, #{is_superuser => IsSuperuser}, ServerFinalMessage};
        {error, _Reason} ->
            {error, not_authorized}
    end.

add_user(UserGroup, UserID, Password, IsSuperuser, State) ->
    {StoredKey, ServerKey, Salt} = esasl_scram:generate_authentication_info(Password, State),
    UserInfo = #user_info{
        user_id = {UserGroup, UserID},
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser
    },
    mnesia:write(?TAB, UserInfo, write).

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
trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case mria:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
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
