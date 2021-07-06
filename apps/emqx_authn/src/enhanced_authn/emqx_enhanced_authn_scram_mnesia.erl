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
-include_lib("esasl/include/esasl_scram.hrl").

-export([ create/3
        , update/4
        , authenticate/5
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

-rlog_shard({?AUTH_SHARD, ?TAB}).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {disc_copies, [node()]},
                {record_name, scram_user_credentail},
                {attributes, record_info(fields, scram_user_credentail)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(Profile, Authenticator, #{algorithm := Algorithm,
                                 iteration_count := IterationCount}) ->
    State = #{user_group => {Profile, Authenticator},
              algorithm => Algorithm,
              iteration_count => IterationCount},
    {ok, State}.

update(_Profile, _Authenticator, _Config, _State) ->
    {error, update_not_suppored}.

%% 1. emqx_authn:enhanced_authenticate(Method, Data, Cache, State).
%% 2. emqx_enhanced_authn_scram:authenticate(Data, Cache, State).
%% 3. emqx_enhanced_authn_scram_mnesia:authenticate(ProfileName, AuthenticatorName, Data, Cache, State)
authenticate(_Profile, _Authenticator, Data, Cache, State) when map_size(Cache) =:= 0 ->
    check_client_first_message(Data, Cache, State);
authenticate(_Profile, _Authenticator, Data, #{next_step := client_final} = Cache, State) ->
    check_client_final_message(Data, Cache, State).

destroy(#{user_group := UserGroup}) ->
    trans(
        fun() ->
            MatchSpec = [{{scram_user_credentail, {UserGroup, '_'}, '_', '_', '_'}, [], ['$_']}],
            ok = lists:foreach(fun(UserCredential) ->
                                  mnesia:delete_object(?TAB, UserCredential, write)
                               end, mnesia:select(?TAB, MatchSpec, write))
        end).

%% TODO: binary to atom
add_user(#{<<"user_id">> := UserID,
           <<"password">> := Password}, #{user_group := UserGroup} = State) ->
    trans(
        fun() ->
            case mnesia:read(?TAB, {UserGroup, UserID}, write) of
                [] ->
                    add_user(UserID, Password, State),
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
                    add_user(UserID, Password, State),
                    {ok, #{user_id => UserID}}
            end
        end).

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [#scram_user_credentail{user_id = {_, UserID}}] ->
            {ok, #{user_id => UserID}};
        [] ->
            {error, not_found}
    end.

%% TODO: Support Pagination
list_users(#{user_group := UserGroup}) ->
    Users = [#{user_id => UserID} ||
                 #scram_user_credentail{user_id = {UserGroup0, UserID}} <- ets:tab2list(?TAB), UserGroup0 =:= UserGroup],
    {ok, Users}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

check_client_first_message(Bin, _Cache, #{iteration_count := IterationCount} = State) ->
    LookupFun = fun(Username) ->
                    lookup_user2(Username, State)
                end,
    esasl_scram:check_client_first_message(
        Bin,
        #{iteration_count => IterationCount,
          lookup => LookupFun}
    ).

check_client_final_message(Bin, Cache, #{algorithm := Alg}) ->
    esasl_scram:check_client_final_message(Bin, Cache#{algorithm => Alg}).

add_user(UserID, Password, State) ->
    UserCredential = esasl_scram:generate_user_credential(UserID, Password, State),
    mnesia:write(?TAB, UserCredential, write).

lookup_user2(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [#scram_user_credentail{} = UserCredential] ->
            {ok, UserCredential};
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