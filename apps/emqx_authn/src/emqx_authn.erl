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

-module(emqx_authn).

-include("emqx_authn.hrl").

-export([ enable/0
        , disable/0
        , is_enabled/0
        ]).

-export([authenticate/2]).

-export([ create_chain/1
        , delete_chain/1
        , lookup_chain/1
        , list_chains/0
        , create_authenticator/2
        , delete_authenticator/2
        , update_authenticator/3
        , update_or_create_authenticator/3
        , lookup_authenticator/2
        , list_authenticators/1
        , move_authenticator_to_the_nth/3
        ]).

-export([ import_users/3
        , add_user/3
        , delete_user/3
        , update_user/4
        , lookup_user/3
        , list_users/2
        ]).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(CHAIN_TAB, emqx_authn_chain).

-rlog_shard({?AUTH_SHARD, ?CHAIN_TAB}).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true}]}],
    %% Chain table
    ok = ekka_mnesia:create_table(?CHAIN_TAB, [
                {ram_copies, [node()]},
                {record_name, chain},
                {local_content, true},
                {attributes, record_info(fields, chain)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?CHAIN_TAB, ram_copies).

enable() ->
    case emqx:hook('client.authenticate', {?MODULE, authenticate, []}) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

disable() ->
    emqx:unhook('client.authenticate', {?MODULE, authenticate, []}),
    ok.

is_enabled() ->
    Callbacks = emqx_hooks:lookup('client.authenticate'),
    lists:any(fun({callback, {?MODULE, authenticate, []}, _, _}) ->
                  true;
                 (_) ->
                  false
              end, Callbacks).

authenticate(Credential, _AuthResult) ->
    case mnesia:dirty_read(?CHAIN_TAB, ?CHAIN) of
        [#chain{authenticators = Authenticators}] ->
            do_authenticate(Authenticators, Credential);
        [] ->
            {stop, {error, not_authorized}}
    end.

do_authenticate([], _) ->
    {stop, {error, not_authorized}};
do_authenticate([{_, _, #authenticator{provider = Provider, state = State}} | More], Credential) ->
    case Provider:authenticate(Credential, State) of
        ignore ->
            do_authenticate(More, Credential);
        Result ->
            %% ok
            %% {ok, AuthData}
            %% {continue, AuthCache}
            %% {continue, AuthData, AuthCache}
            %% {error, Reason}
            {stop, Result}
    end.

create_chain(#{id := ID}) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ID, write) of
                [] ->
                    Chain = #chain{id = ID,
                                   authenticators = [],
                                   created_at = erlang:system_time(millisecond)},
                    mnesia:write(?CHAIN_TAB, Chain, write),
                    {ok, serialize_chain(Chain)};
                [_ | _] ->
                    {error, {already_exists, {chain, ID}}}
            end
        end).

delete_chain(ID) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ID, write) of
                [] ->
                    {error, {not_found, {chain, ID}}};
                [#chain{authenticators = Authenticators}] ->
                    _ = [do_delete_authenticator(Authenticator) || {_, _, Authenticator} <- Authenticators],
                    mnesia:delete(?CHAIN_TAB, ID, write)
            end
        end).

lookup_chain(ID) ->
    case mnesia:dirty_read(?CHAIN_TAB, ID) of
        [] ->
            {error, {not_found, {chain, ID}}};
        [Chain] ->
            {ok, serialize_chain(Chain)}
    end.

list_chains() ->
    Chains = ets:tab2list(?CHAIN_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

create_authenticator(ChainID, #{name := Name} = Config) ->
    UpdateFun =
        fun(Chain = #chain{authenticators = Authenticators}) ->
            case lists:keymember(Name, 2, Authenticators) of
                true ->
                    {error, name_has_be_used};
                false ->
                    AlreadyExist = fun(ID) ->
                                       lists:keymember(ID, 1, Authenticators)
                                   end,
                    AuthenticatorID = gen_id(AlreadyExist),
                    case do_create_authenticator(ChainID, AuthenticatorID, Config) of
                        {ok, Authenticator} ->
                            NAuthenticators = Authenticators ++ [{AuthenticatorID, Name, Authenticator}],
                            ok = mnesia:write(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}, write),
                            {ok, serialize_authenticator(Authenticator)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
        end,
    update_chain(ChainID, UpdateFun).

delete_authenticator(ChainID, AuthenticatorID) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case lists:keytake(AuthenticatorID, 1, Authenticators) of
                        false ->
                            {error, {not_found, {authenticator, AuthenticatorID}}};
                        {value, {_, _, Authenticator}, NAuthenticators} ->
                            _ = do_delete_authenticator(Authenticator),
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write)
                    end
                end,
    update_chain(ChainID, UpdateFun).

update_authenticator(ChainID, AuthenticatorID, Config) ->
    do_update_authenticator(ChainID, AuthenticatorID, Config, false).

update_or_create_authenticator(ChainID, AuthenticatorID, Config) ->
    do_update_authenticator(ChainID, AuthenticatorID, Config, true).

do_update_authenticator(ChainID, AuthenticatorID, #{name := NewName} = Config, CreateWhenNotFound) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case lists:keytake(AuthenticatorID, 1, Authenticators) of
                        false ->
                            case CreateWhenNotFound of
                                true ->
                                    case lists:keymember(NewName, 2, Authenticators) of
                                        true ->
                                            {error, name_has_be_used};
                                        false ->
                                            case do_create_authenticator(ChainID, AuthenticatorID, Config) of
                                                {ok, Authenticator} ->
                                                    NAuthenticators = Authenticators ++ [{AuthenticatorID, NewName, Authenticator}],
                                                    ok = mnesia:write(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}, write),
                                                    {ok, serialize_authenticator(Authenticator)};
                                                {error, Reason} ->
                                                    {error, Reason}
                                            end
                                        end;
                                false ->
                                    {error, {not_found, {authenticator, AuthenticatorID}}}
                            end;
                        {value,
                         {_, _, #authenticator{provider = Provider,
                                               state    = #{version := Version} = State} = Authenticator},
                         Others} ->
                            case lists:keymember(NewName, 2, Others) of
                                true ->
                                    {error, name_has_be_used};
                                false ->
                                    case (NewProvider = authenticator_provider(Config)) =:= Provider of
                                        true ->
                                            Unique = <<ChainID/binary, "/", AuthenticatorID/binary, ":", Version/binary>>,
                                            case Provider:update(Config#{'_unique' => Unique}, State) of
                                                {ok, NewState} ->
                                                    NewAuthenticator = Authenticator#authenticator{name = NewName,
                                                                                                   config = Config,
                                                                                                   state = switch_version(NewState)},
                                                    NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                                    ok = mnesia:write(?CHAIN_TAB, Chain#chain{authenticators = NewAuthenticators}, write),
                                                    {ok, serialize_authenticator(NewAuthenticator)};
                                                {error, Reason} ->
                                                    {error, Reason}
                                            end;
                                        false ->
                                            Unique = <<ChainID/binary, "/", AuthenticatorID/binary, ":", Version/binary>>,
                                            case NewProvider:create(Config#{'_unique' => Unique}) of
                                                {ok, NewState} ->
                                                    NewAuthenticator = Authenticator#authenticator{name = NewName,
                                                                                                   provider = NewProvider,
                                                                                                   config = Config,
                                                                                                   state = switch_version(NewState)},
                                                    NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                                    ok = mnesia:write(?CHAIN_TAB, Chain#chain{authenticators = NewAuthenticators}, write),
                                                    _ = Provider:destroy(State),
                                                    {ok, serialize_authenticator(NewAuthenticator)};
                                                {error, Reason} ->
                                                    {error, Reason}
                                            end
                                    end
                            end
                    end
                end,
    update_chain(ChainID, UpdateFun).

lookup_authenticator(ChainID, AuthenticatorID) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            case lists:keyfind(AuthenticatorID, 1, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                {_, _, Authenticator} ->
                    {ok, serialize_authenticator(Authenticator)}
            end
    end.

list_authenticators(ChainID) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            {ok, serialize_authenticators(Authenticators)}
    end.

move_authenticator_to_the_nth(ChainID, AuthenticatorID, N) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case move_authenticator_to_the_nth_(AuthenticatorID, Authenticators, N) of
                        {ok, NAuthenticators} ->
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

import_users(ChainID, AuthenticatorID, Filename) ->
    call_authenticator(ChainID, AuthenticatorID, import_users, [Filename]).

add_user(ChainID, AuthenticatorID, UserInfo) ->
    call_authenticator(ChainID, AuthenticatorID, add_user, [UserInfo]).

delete_user(ChainID, AuthenticatorID, UserID) ->
    call_authenticator(ChainID, AuthenticatorID, delete_user, [UserID]).

update_user(ChainID, AuthenticatorID, UserID, NewUserInfo) ->
    call_authenticator(ChainID, AuthenticatorID, update_user, [UserID, NewUserInfo]).

lookup_user(ChainID, AuthenticatorID, UserID) ->
    call_authenticator(ChainID, AuthenticatorID, lookup_user, [UserID]).

list_users(ChainID, AuthenticatorID) ->
    call_authenticator(ChainID, AuthenticatorID, list_users, []).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

authenticator_provider(#{mechanism := 'password-based', server_type := 'built-in-database'}) ->
    emqx_authn_mnesia;
authenticator_provider(#{mechanism := 'password-based', server_type := 'mysql'}) ->
    emqx_authn_mysql;
authenticator_provider(#{mechanism := 'password-based', server_type := 'pgsql'}) ->
    emqx_authn_pgsql;
authenticator_provider(#{mechanism := 'password-based', server_type := 'mongodb'}) ->
    emqx_authn_mongodb;
authenticator_provider(#{mechanism := 'password-based', server_type := 'redis'}) ->
    emqx_authn_redis;
authenticator_provider(#{mechanism := 'password-based', server_type := 'http-server'}) ->
    emqx_authn_http;
authenticator_provider(#{mechanism := jwt}) ->
    emqx_authn_jwt;
authenticator_provider(#{mechanism := scram, server_type := 'built-in-database'}) ->
    emqx_enhanced_authn_scram_mnesia.

gen_id(AlreadyExist) ->
    ID = list_to_binary(emqx_rule_id:gen()),
    case AlreadyExist(ID) of
        true -> gen_id(AlreadyExist);
        false -> ID
    end.

switch_version(State = #{version := ?VER_1}) ->
    State#{version := ?VER_2};
switch_version(State = #{version := ?VER_2}) ->
    State#{version := ?VER_1};
switch_version(State) ->
    State#{version => ?VER_1}.

do_create_authenticator(ChainID, AuthenticatorID, #{name := Name} = Config) ->
    Provider = authenticator_provider(Config),
    Unique = <<ChainID/binary, "/", AuthenticatorID/binary, ":", ?VER_1/binary>>,
    case Provider:create(Config#{'_unique' => Unique}) of
        {ok, State} ->
            Authenticator = #authenticator{id = AuthenticatorID,
                                           name = Name,
                                           provider = Provider,
                                           config = Config,
                                           state = switch_version(State)},
            {ok, Authenticator};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete_authenticator(#authenticator{provider = Provider, state = State}) ->
    _ = Provider:destroy(State),
    ok.
    
replace_authenticator(ID, #authenticator{name = Name} = Authenticator, Authenticators) ->
    lists:keyreplace(ID, 1, Authenticators, {ID, Name, Authenticator}).

move_authenticator_to_the_nth_(AuthenticatorID, Authenticators, N)
  when N =< length(Authenticators) andalso N > 0 ->
    move_authenticator_to_the_nth_(AuthenticatorID, Authenticators, N, []);
move_authenticator_to_the_nth_(_, _, _) ->
    {error, out_of_range}.

move_authenticator_to_the_nth_(AuthenticatorID, [], _, _) ->
    {error, {not_found, {authenticator, AuthenticatorID}}};
move_authenticator_to_the_nth_(AuthenticatorID, [{AuthenticatorID, _, _} = Authenticator | More], N, Passed)
  when N =< length(Passed) ->
    {L1, L2} = lists:split(N - 1, lists:reverse(Passed)),
    {ok, L1 ++ [Authenticator] ++ L2 ++ More};
move_authenticator_to_the_nth_(AuthenticatorID, [{AuthenticatorID, _, _} = Authenticator | More], N, Passed) ->
    {L1, L2} = lists:split(N - length(Passed) - 1, More),
    {ok, lists:reverse(Passed) ++ L1 ++ [Authenticator] ++ L2};
move_authenticator_to_the_nth_(AuthenticatorID, [Authenticator | More], N, Passed) ->
    move_authenticator_to_the_nth_(AuthenticatorID, More, N, [Authenticator | Passed]).

update_chain(ChainID, UpdateFun) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ChainID, write) of
                [] ->
                    {error, {not_found, {chain, ChainID}}};
                [Chain] ->
                    UpdateFun(Chain)
            end
        end).

call_authenticator(ChainID, AuthenticatorID, Func, Args) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            case lists:keyfind(AuthenticatorID, 1, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                {_, _, #authenticator{provider = Provider, state = State}} ->
                    case erlang:function_exported(Provider, Func, length(Args) + 1) of
                        true ->
                            erlang:apply(Provider, Func, Args ++ [State]);
                        false ->
                            {error, unsupported_feature}
                    end
            end
    end.

serialize_chain(#chain{id = ID,
                       authenticators = Authenticators,
                       created_at = CreatedAt}) ->
    #{id => ID,
      authenticators => serialize_authenticators(Authenticators),
      created_at => CreatedAt}.

serialize_authenticators(Authenticators) ->
    [serialize_authenticator(Authenticator) || {_, _, Authenticator} <- Authenticators].

serialize_authenticator(#authenticator{id = ID,
                                       config = Config}) ->
    Config#{id => ID}.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.
