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

-module(emqx_enhanced_authn_scram_mnesia).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1
        ]).

-export([ create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

-export([ add_user/2
        , delete_user/2
        , update_user/3
        , lookup_user/2
        , list_users/1
        ]).

-define(TAB, ?MODULE).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-record(user_info,
        { user_id
        , stored_key
        , server_key
        , salt
        , superuser
        }).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {rlog_shard, ?AUTH_SHARD},
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
    [ {name,            fun emqx_authn_schema:authenticator_name/1}
    , {mechanism,       {enum, [scram]}}
    , {server_type,     fun server_type/1}
    , {algorithm,       fun algorithm/1}
    , {iteration_count, fun iteration_count/1}
    ].

server_type(type) -> hoconsc:enum(['built-in-database']);
server_type(default) -> 'built-in-database';
server_type(_) -> undefined.

algorithm(type) -> hoconsc:enum([sha256, sha512]);
algorithm(default) -> sha256;
algorithm(_) -> undefined.

iteration_count(type) -> non_neg_integer();
iteration_count(default) -> 4096;
iteration_count(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{ algorithm := Algorithm
        , iteration_count := IterationCount
        , '_unique' := Unique
        }) ->
    State = #{user_group => Unique,
              algorithm => Algorithm,
              iteration_count => IterationCount},
    {ok, State}.

update(Config, #{user_group := Unique}) ->
    create(Config#{'_unique' => Unique}).

authenticate(#{auth_method := AuthMethod,
               auth_data := AuthData,
               auth_cache := AuthCache}, State) ->
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
    trans(
        fun() ->
            MatchSpec = [{{user_info, {UserGroup, '_'}, '_', '_', '_', '_'}, [], ['$_']}],
            ok = lists:foreach(fun(UserInfo) ->
                                  mnesia:delete_object(?TAB, UserInfo, write)
                               end, mnesia:select(?TAB, MatchSpec, write))
        end).

add_user(#{user_id := UserID,
           password := Password} = UserInfo, #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    Superuser = maps:get(superuser, UserInfo, false),
                    add_user(UserID, Password, Superuser, State),
                    {ok, #{user_id => UserID, superuser => Superuser}};
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

update_user(UserID, User,
            #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    {error, not_found};
                [#user_info{superuser = Superuser} = UserInfo] ->
                    UserInfo1 = UserInfo#user_info{superuser = maps:get(superuser, User, Superuser)},
                    UserInfo2 = case maps:get(password, User, undefined) of
                                    undefined ->
                                        UserInfo1;
                                    Password ->
                                        {StoredKey, ServerKey, Salt} = esasl_scram:generate_authentication_info(Password, State),
                                        UserInfo1#user_info{stored_key = StoredKey,
                                                            server_key = ServerKey,
                                                            salt       = Salt}
                                end,
                    mnesia:write(?TAB, UserInfo2, write),
                    {ok, serialize_user_info(UserInfo2)}
            end
        end).

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [UserInfo] ->
            {ok, serialize_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

%% TODO: Support Pagination
list_users(#{user_group := UserGroup}) ->
    Users = [serialize_user_info(UserInfo) ||
                 #user_info{user_id = {UserGroup0, _}} = UserInfo <- ets:tab2list(?TAB), UserGroup0 =:= UserGroup],
    {ok, Users}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

ensure_auth_method('SCRAM-SHA-256', #{algorithm := sha256}) ->
    true;
ensure_auth_method('SCRAM-SHA-512', #{algorithm := sha512}) ->
    true;
ensure_auth_method(_, _) ->
    false.

check_client_first_message(Bin, _Cache, #{iteration_count := IterationCount} = State) ->
    RetrieveFun = fun(Username) ->
                    retrieve(Username, State)
                end,
    case esasl_scram:check_client_first_message(
             Bin,
             #{iteration_count => IterationCount,
               retrieve => RetrieveFun}
         ) of
        {cotinue, ServerFirstMessage, Cache} ->
            {cotinue, ServerFirstMessage, Cache};
        {error, _Reason} ->
            {error, not_authorized}
    end.

check_client_final_message(Bin, #{superuser := Superuser} = Cache, #{algorithm := Alg}) ->
    case esasl_scram:check_client_final_message(
             Bin,
             Cache#{algorithm => Alg}
         ) of
        {ok, ServerFinalMessage} ->
            {ok, #{superuser => Superuser}, ServerFinalMessage};
        {error, _Reason} ->
            {error, not_authorized}
    end.

add_user(UserID, Password, Superuser, State) ->
    {StoredKey, ServerKey, Salt} = esasl_scram:generate_authentication_info(Password, State),
    UserInfo = #user_info{user_id    = UserID,
                          stored_key = StoredKey,
                          server_key = ServerKey,
                          salt       = Salt,
                          superuser  = Superuser},
    mnesia:write(?TAB, UserInfo, write).

retrieve(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [#user_info{stored_key = StoredKey,
                    server_key = ServerKey,
                    salt       = Salt,
                    superuser  = Superuser}] ->
            {ok, #{stored_key => StoredKey,
                   server_key => ServerKey,
                   salt => Salt,
                   superuser => Superuser}};
        [] ->
            {error, not_found}
    end.

%% TODO: Move to emqx_authn_utils.erl
trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

serialize_user_info(#user_info{user_id = {_, UserID}, superuser = Superuser}) ->
    #{user_id => UserID, superuser => Superuser}.
