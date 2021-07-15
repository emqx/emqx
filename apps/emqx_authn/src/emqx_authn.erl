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
        ]).

-export([authenticate/2]).

-export([ create_chain/1
        , delete_chain/1
        , lookup_chain/1
        , list_chains/0
        , create_authenticator/2
        , delete_authenticator/2
        , update_authenticator/3
        , lookup_authenticator/2
        , list_authenticators/1
        , move_authenticator_to_the_front/2
        , move_authenticator_to_the_end/2
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
                {storage_properties, StoreProps}]).

enable() ->
    case emqx:hook('client.authenticate', {?MODULE, authenticate, []}) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

disable() ->
    emqx:unhook('client.authenticate', {?MODULE, authenticate, []}),
    ok.

authenticate(Credential, _AuthResult) ->
    case mnesia:dirty_read(?CHAIN_TAB, ?CHAIN) of
        [#chain{authenticators = Authenticators}] ->
            do_authenticate(Authenticators, Credential);
        [] ->
            {stop, {error, not_authorized}}
    end.

do_authenticate([], _) ->
    {stop, {error, not_authorized}};
do_authenticate([{_, #authenticator{provider = Provider, state = State}} | More], Credential) ->
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
                    _ = [do_delete_authenticator(Authenticator) || {_, Authenticator} <- Authenticators],
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

create_authenticator(ChainID, #{name := Name,
                                mechanism := Mechanism,
                                config := Config}) ->
    UpdateFun =
        fun(Chain = #chain{authenticators = Authenticators}) ->
            case lists:keymember(Name, 1, Authenticators) of
                true ->
                    {error, {already_exists, {authenticator, Name}}};
                false ->
                    Provider = authenticator_provider(Mechanism, Config),
                    case Provider:create(ChainID, Name, Config) of
                        {ok, State} ->
                            Authenticator = #authenticator{name = Name,
                                                           mechanism = Mechanism,
                                                           provider = Provider,
                                                           config = Config,
                                                           state = State},
                            NChain = Chain#chain{authenticators = Authenticators ++ [{Name, Authenticator}]},
                            ok = mnesia:write(?CHAIN_TAB, NChain, write),
                            {ok, serialize_authenticator(Authenticator)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
        end,
    update_chain(ChainID, UpdateFun).

delete_authenticator(ChainID, AuthenticatorName) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case lists:keytake(AuthenticatorName, 1, Authenticators) of
                        false ->
                            {error, {not_found, {authenticator, AuthenticatorName}}};
                        {value, {_, Authenticator}, NAuthenticators} ->
                            _ = do_delete_authenticator(Authenticator),
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write)
                    end
                end,
    update_chain(ChainID, UpdateFun).

update_authenticator(ChainID, AuthenticatorName, Config) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case proplists:get_value(AuthenticatorName, Authenticators, undefined) of
                        undefined ->
                            {error, {not_found, {authenticator, AuthenticatorName}}};
                        #authenticator{provider = Provider,
                                       config   = OriginalConfig,
                                       state    = State} = Authenticator ->
                            NewConfig = maps:merge(OriginalConfig, Config),
                            case Provider:update(ChainID, AuthenticatorName, NewConfig, State) of
                                {ok, NState} ->
                                    NAuthenticator = Authenticator#authenticator{config = NewConfig,
                                                                                 state = NState},
                                    NAuthenticators = update_value(AuthenticatorName, NAuthenticator, Authenticators),
                                    ok = mnesia:write(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}, write),
                                    {ok, serialize_authenticator(NAuthenticator)};
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end
                 end,
    update_chain(ChainID, UpdateFun).

lookup_authenticator(ChainID, AuthenticatorName) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            case proplists:get_value(AuthenticatorName, Authenticators, undefined) of
                undefined ->
                    {error, {not_found, {authenticator, AuthenticatorName}}};
                Authenticator ->
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

move_authenticator_to_the_front(ChainID, AuthenticatorName) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case move_authenticator_to_the_front_(AuthenticatorName, Authenticators) of
                        {ok, NAuthenticators} ->
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_authenticator_to_the_end(ChainID, AuthenticatorName) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case move_authenticator_to_the_end_(AuthenticatorName, Authenticators) of
                        {ok, NAuthenticators} ->
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_authenticator_to_the_nth(ChainID, AuthenticatorName, N) ->
    UpdateFun = fun(Chain = #chain{authenticators = Authenticators}) ->
                    case move_authenticator_to_the_nth_(AuthenticatorName, Authenticators, N) of
                        {ok, NAuthenticators} ->
                            NChain = Chain#chain{authenticators = NAuthenticators},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

import_users(ChainID, AuthenticatorName, Filename) ->
    call_authenticator(ChainID, AuthenticatorName, import_users, [Filename]).

add_user(ChainID, AuthenticatorName, UserInfo) ->
    call_authenticator(ChainID, AuthenticatorName, add_user, [UserInfo]).

delete_user(ChainID, AuthenticatorName, UserID) ->
    call_authenticator(ChainID, AuthenticatorName, delete_user, [UserID]).

update_user(ChainID, AuthenticatorName, UserID, NewUserInfo) ->
    call_authenticator(ChainID, AuthenticatorName, update_user, [UserID, NewUserInfo]).

lookup_user(ChainID, AuthenticatorName, UserID) ->
    call_authenticator(ChainID, AuthenticatorName, lookup_user, [UserID]).

list_users(ChainID, AuthenticatorName) ->
    call_authenticator(ChainID, AuthenticatorName, list_users, []).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

authenticator_provider('password-based', #{server_type := 'built-in-database'}) ->
    emqx_authn_mnesia;
authenticator_provider('password-based', #{server_type := 'mysql'}) ->
    emqx_authn_mysql;
authenticator_provider('password-based', #{server_type := 'pgsql'}) ->
    emqx_authn_pgsql;
authenticator_provider('password-based', #{server_type := 'http-server'}) ->
    emqx_authn_http;
authenticator_provider(jwt, _) ->
    emqx_authn_jwt;
authenticator_provider(scram, #{server_type := 'built-in-database'}) ->
    emqx_enhanced_authn_scram_mnesia.

do_delete_authenticator(#authenticator{provider = Provider, state = State}) ->
    Provider:destroy(State).
    
update_value(Key, Value, List) ->
    lists:keyreplace(Key, 1, List, {Key, Value}).

move_authenticator_to_the_front_(AuthenticatorName, Authenticators) ->
    move_authenticator_to_the_front_(AuthenticatorName, Authenticators, []).

move_authenticator_to_the_front_(AuthenticatorName, [], _) ->
    {error, {not_found, {authenticator, AuthenticatorName}}};
move_authenticator_to_the_front_(AuthenticatorName, [{AuthenticatorName, _} = Authenticator | More], Passed) ->
    {ok, [Authenticator | (lists:reverse(Passed) ++ More)]};
move_authenticator_to_the_front_(AuthenticatorName, [Authenticator | More], Passed) ->
    move_authenticator_to_the_front_(AuthenticatorName, More, [Authenticator | Passed]).

move_authenticator_to_the_end_(AuthenticatorName, Authenticators) ->
    move_authenticator_to_the_end_(AuthenticatorName, Authenticators, []).

move_authenticator_to_the_end_(AuthenticatorName, [], _) ->
    {error, {not_found, {authenticator, AuthenticatorName}}};
move_authenticator_to_the_end_(AuthenticatorName, [{AuthenticatorName, _} = Authenticator | More], Passed) ->
    {ok, lists:reverse(Passed) ++ More ++ [Authenticator]};
move_authenticator_to_the_end_(AuthenticatorName, [Authenticator | More], Passed) ->
    move_authenticator_to_the_end_(AuthenticatorName, More, [Authenticator | Passed]).

move_authenticator_to_the_nth_(AuthenticatorName, Authenticators, N)
  when N =< length(Authenticators) andalso N > 0 ->
    move_authenticator_to_the_nth_(AuthenticatorName, Authenticators, N, []);
move_authenticator_to_the_nth_(_, _, _) ->
    {error, out_of_range}.

move_authenticator_to_the_nth_(AuthenticatorName, [], _, _) ->
    {error, {not_found, {authenticator, AuthenticatorName}}};
move_authenticator_to_the_nth_(AuthenticatorName, [{AuthenticatorName, _} = Authenticator | More], N, Passed)
  when N =< length(Passed) ->
    {L1, L2} = lists:split(N - 1, lists:reverse(Passed)),
    {ok, L1 ++ [Authenticator] ++ L2 ++ More};
move_authenticator_to_the_nth_(AuthenticatorName, [{AuthenticatorName, _} = Authenticator | More], N, Passed) ->
    {L1, L2} = lists:split(N - length(Passed) - 1, More),
    {ok, lists:reverse(Passed) ++ L1 ++ [Authenticator] ++ L2};
move_authenticator_to_the_nth_(AuthenticatorName, [Authenticator | More], N, Passed) ->
    move_authenticator_to_the_nth_(AuthenticatorName, More, N, [Authenticator | Passed]).

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

% lookup_chain_by_listener(ListenerID, AuthNType) ->
%     case mnesia:dirty_read(?BINDING_TAB, {ListenerID, AuthNType}) of
%         [] ->
%             {error, not_found};
%         [#binding{chain_id = ChainID}] ->
%             {ok, ChainID}
%     end.


call_authenticator(ChainID, AuthenticatorName, Func, Args) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            case proplists:get_value(AuthenticatorName, Authenticators, undefined) of
                undefined ->
                    {error, {not_found, {authenticator, AuthenticatorName}}};
                #authenticator{provider = Provider, state = State} ->
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

% serialize_binding(#binding{bound = {ListenerID, _},
%                            chain_id = ChainID}) ->
%     #{listener_id => ListenerID,
%       chain_id => ChainID}.

serialize_authenticators(Authenticators) ->
    [serialize_authenticator(Authenticator) || {_, Authenticator} <- Authenticators].

serialize_authenticator(#authenticator{name = Name,
                                       mechanism = Mechanism,
                                       config = Config}) ->
    #{name => Name,
      mechanism => Mechanism,
      config => Config}.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.
