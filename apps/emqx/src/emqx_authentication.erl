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

-module(emqx_authentication).

-behaviour(gen_server).

-behaviour(emqx_config_handler).

-include("emqx.hrl").
-include("logger.hrl").

% -export([ pre_config_update/2
%         , post_config_update/3
%         , update_config/2
%         ]).

-export([authenticate/2]).

-export([ start_link/0
        , stop/0
        ]).

-export([ add_provider/2
        , create_chain/1
        , delete_chain/1
        , lookup_chain/1
        , list_chains/0
        , create_authenticator/2
        , delete_authenticator/2
        , update_authenticator/3
        , update_or_create_authenticator/3
        , lookup_authenticator/2
        , list_authenticators/1
        , move_authenticator/3
        ]).

-export([ import_users/3
        , add_user/3
        , delete_user/3
        , update_user/4
        , lookup_user/3
        , list_users/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(CHAINS_TAB, emqx_authn_chains).

-define(ALL_LISTENERS, <<"all">>).

-define(VER_1, <<"1">>).
-define(VER_2, <<"2">>).

-type config() :: #{atom() => term()}.
-type state() :: #{atom() => term()}.
-type extra() :: #{superuser := boolean(),
                   atom() => term()}.
-type user_info() :: #{user_id := binary(),
                       atom() => term()}.

-callback create(Config)
    -> {ok, State}
     | {error, term()}
    when Config::config(), State::state().

-callback update(Config, State)
    -> {ok, NewState}
     | {error, term()}
    when Config::config(), State::state(), NewState::state().

-callback authenticate(Credential, State)
    -> ignore
     | {ok, Extra}
     | {ok, Extra, AuthData}
     | {continue, AuthCache}
     | {continue, AuthData, AuthCache}
     | {error, term()}
  when Credential::map(), State::state(), Extra::extra(), AuthData::binary(), AuthCache::map().

-callback destroy(State)
    -> ok
    when State::state().

-callback import_users(Filename, State)
    -> ok
     | {error, term()}
    when Filename::binary(), State::state().

-callback add_user(UserInfo, State)
    -> {ok, User}
     | {error, term()}
    when UserInfo::user_info(), State::state(), User::user_info().

-callback delete_user(UserID, State)
    -> ok
     | {error, term()}
    when UserID::binary(), State::state().

-callback update_user(UserID, UserInfo, State)
    -> {ok, User}
     | {error, term()}
    when UserID::binary, UserInfo::map(), State::state(), User::user_info().

-callback list_users(State)
    -> {ok, Users}
    when State::state(), Users::[user_info()].

-optional_callbacks([ import_users/2
                    , add_user/2
                    , delete_user/2
                    , update_user/3
                    , list_users/1
                    ]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

% pre_config_update({create_authenticator, Config}, OldConfig) ->
%     {ok, OldConfig ++ [Config]};
% pre_config_update({delete_authenticator, ID}, OldConfig) ->
%     case lookup_authenticator(?CHAIN, ID) of
%         {error, Reason} -> {error, Reason};
%         {ok, #{name := Name}} ->
%             NewConfig = lists:filter(fun(#{<<"name">> := N}) ->
%                                          N =/= Name
%                                      end, OldConfig),
%             {ok, NewConfig}
%     end;
% pre_config_update({update_authenticator, ID, Config}, OldConfig) ->
%     case lookup_authenticator(?CHAIN, ID) of
%         {error, Reason} -> {error, Reason};
%         {ok, #{name := Name}} ->
%             NewConfig = lists:map(fun(#{<<"name">> := N} = C) ->
%                                       case N =:= Name of
%                                           true -> maps:merge(C, Config);
%                                           false -> C
%                                       end
%                                   end, OldConfig),
%             {ok, NewConfig}
%     end;
% pre_config_update({update_or_create_authenticator, ID, Config}, OldConfig) ->
%     case lookup_authenticator(?CHAIN, ID) of
%         {error, _Reason} -> OldConfig ++ [Config];
%         {ok, #{name := Name}} ->
%             NewConfig = lists:map(fun(#{<<"name">> := N} = C) ->
%                                       case N =:= Name of
%                                           true -> Config;
%                                           false -> C
%                                       end
%                                   end, OldConfig),
%             {ok, NewConfig}
%     end;
% pre_config_update({move_authenticator, ID, Position}, OldConfig) ->
%     case lookup_authenticator(?CHAIN, ID) of
%         {error, Reason} -> {error, Reason};
%         {ok, #{name := Name}} ->
%             {ok, Found, Part1, Part2} = split_by_name(Name, OldConfig),
%             case Position of
%                 <<"top">> ->
%                     {ok, [Found | Part1] ++ Part2};
%                 <<"bottom">> ->
%                     {ok, Part1 ++ Part2 ++ [Found]};
%                 Before ->
%                     case binary:split(Before, <<":">>, [global]) of
%                         [<<"before">>, ID0] ->
%                             case lookup_authenticator(?CHAIN, ID0) of
%                                 {error, Reason} -> {error, Reason};
%                                 {ok, #{name := Name1}} ->
%                                     {ok, NFound, NPart1, NPart2} = split_by_name(Name1, Part1 ++ Part2),
%                                     {ok, NPart1 ++ [Found, NFound | NPart2]}
%                             end;
%                         _ ->
%                             {error, {invalid_parameter, position}}
%                     end
%             end
%     end.

% post_config_update({create_authenticator, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
%     case lists:filter(
%              fun(#{name := N}) ->
%                  N =:= Name
%              end, NewConfig) of
%         [Config] ->
%             create_authenticator(?CHAIN, Config);
%         [_Config | _] ->
%             {error, name_has_be_used}
%     end;
% post_config_update({delete_authenticator, ID}, _NewConfig, _OldConfig) ->
%     case delete_authenticator(?CHAIN, ID) of
%         ok -> ok;
%         {error, Reason} -> throw(Reason)
%     end;
% post_config_update({update_authenticator, ID, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
%     case lists:filter(
%              fun(#{name := N}) ->
%                  N =:= Name
%              end, NewConfig) of
%         [Config] ->
%             update_authenticator(?CHAIN, ID, Config);
%         [_Config | _] ->
%             {error, name_has_be_used}
%     end;
% post_config_update({update_or_create_authenticator, ID, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
%     case lists:filter(
%              fun(#{name := N}) ->
%                  N =:= Name
%              end, NewConfig) of
%         [Config] ->
%             update_or_create_authenticator(?CHAIN, ID, Config);
%         [_Config | _] ->
%             {error, name_has_be_used}
%     end;
% post_config_update({move_authenticator, ID, Position}, _NewConfig, _OldConfig) ->
%     NPosition = case Position of
%                     <<"top">> -> top;
%                     <<"bottom">> -> bottom;
%                     Before ->
%                         case binary:split(Before, <<":">>, [global]) of
%                             [<<"before">>, ID0] ->
%                                 {before, ID0};
%                             _ ->
%                                 {error, {invalid_parameter, position}}
%                         end
%                 end,
%     move_authenticator(?CHAIN, ID, NPosition).

% update_config(Path, ConfigRequest) ->
%     emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).


%%------------------------------------------------------------------------------
%% Listeners
%%------------------------------------------------------------------------------

% create_listener(ListenerName) ->
%     create_authentcaiton_for_listener(ListenerName),
%     do_create_listener(ListenerName).

% create_authentcaiton_for_listener(ListenerName) ->
%     case emqx_config:get([listeners, ListenerName, authentication], []) of
%         [] ->
%             case emqx_config:get([authentication], []) of
%                 [] ->
%                     ok;
%                 AuthenticationConfig0 ->
%                     emqx_authentication:create_chain(ListenerName),
%                     emqx_authentication:create_authenticators(ListenerName, AuthenticationConfig0)
%             end;
%         AuthenticationConfig ->
%             emqx_authentication:create_chain(ListenerName),
%             emqx_authentication:create_authenticators(ListenerName, AuthenticationConfig)
%     end.

% delete_listener(ListenerName) ->
%     do_delete_listener(ListenerName),
%     delete_authentcaiton_for_listener(ListenerName).

% delete_authentcaiton_for_listener(ListenerName) ->
%     emqx_authentication:delete_chain(ListenerName).


% initialize() ->
%     AuthNConfig = emqx:get_config([authentication], []),
%     initialize(AuthNConfig).

% initialize(AuthNConfig) ->
%     {ok, _} = create_chain(?ALL_LISTENERS),
%     lists:foreach(fun(#{name := Name} = AuthNConfig0) ->
%                       case create_authenticator({mqtt, ?ALL_LISTENERS}, AuthNConfig0) of
%                           {ok, _} ->
%                             ok;
%                           {error, Reason} ->
%                             ?LOG(error, "Failed to create authenticator '~s': ~p", [Name, Reason])
%                       end
%                   end, AuthNConfig),
%     ok.

authenticate(#{listener := Listener, protocol := Protocol} = Credential, _AuthResult) ->
    case ets:lookup(?CHAINS_TAB, Listener) of
        [#chain{authenticators = Authenticators}] when Authenticators =/= [] ->
            do_authenticate(Authenticators, Credential);
        _ ->
            case ets:lookup(?CHAINS_TAB, {Protocol, ?ALL_LISTENERS}) of
                [#chain{authenticators = Authenticators}] when Authenticators =/= [] ->
                    do_authenticate(Authenticators, Credential);
                _ ->
                    ignore
            end
    end.

do_authenticate([], _) ->
    {stop, {error, not_authorized}};
do_authenticate([#authenticator{provider = Provider, state = State} | More], Credential) ->
    case Provider:authenticate(Credential, State) of
        ignore ->
            do_authenticate(More, Credential);
        Result ->
            %% {ok, Extra}
            %% {ok, Extra, AuthData}
            %% {ok, MetaData}
            %% {continue, AuthCache}
            %% {continue, AuthData, AuthCache}
            %% {error, Reason}
            {stop, Result}
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

add_provider(AuthNType, Provider) ->
    gen_server:call(?MODULE, {add_provider, AuthNType, Provider}).

create_chain(Name) ->
    gen_server:call(?MODULE, {create_chain, Name}).

delete_chain(Name) ->
    gen_server:call(?MODULE, {delete_chain, Name}).

lookup_chain(Name) ->
    gen_server:call(?MODULE, {lookup_chain, Name}).

list_chains() ->
    Chains = ets:tab2list(?CHAINS_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

create_authenticator(ChainName, Config) ->
    gen_server:call(?MODULE, {create_authenticator, ChainName, Config}).

delete_authenticator(ChainName, AuthenticatorID) ->
    gen_server:call(?MODULE, {delete_authenticator, ChainName, AuthenticatorID}).

update_authenticator(ChainName, AuthenticatorID, Config) ->
    gen_server:call(?MODULE, {update_authenticator, ChainName, AuthenticatorID, Config}).

update_or_create_authenticator(ChainName, AuthenticatorID, Config) ->
    gen_server:call(?MODULE, {update_or_create_authenticator, ChainName, AuthenticatorID, Config}).

lookup_authenticator(ChainName, AuthenticatorID) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [#chain{authenticators = Authenticators}] ->
            case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                {_, _, Authenticator} ->
                    {ok, serialize_authenticator(Authenticator)}
            end
    end.

list_authenticators(ChainName) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [#chain{authenticators = Authenticators}] ->
            {ok, serialize_authenticators(Authenticators)}
    end.

move_authenticator(ChainName, AuthenticatorID, Position) ->
    gen_server:call(?MODULE, {move_authenticator, ChainName, AuthenticatorID, Position}).

import_users(ChainName, AuthenticatorID, Filename) ->
    gen_server:call(?MODULE, {import_users, ChainName, AuthenticatorID, Filename}).

add_user(ChainName, AuthenticatorID, UserInfo) ->
    gen_server:call(?MODULE, {add_user, ChainName, AuthenticatorID, UserInfo}).

delete_user(ChainName, AuthenticatorID, UserID) ->
    gen_server:call(?MODULE, {delete_user, ChainName, AuthenticatorID, UserID}).

update_user(ChainName, AuthenticatorID, UserID, NewUserInfo) ->
    gen_server:call(?MODULE, {update_user, ChainName, AuthenticatorID, UserID, NewUserInfo}).

lookup_user(ChainName, AuthenticatorID, UserID) ->
    gen_server:call(?MODULE, {lookup_user, ChainName, AuthenticatorID, UserID}).

%% TODO: Support pagination
list_users(ChainName, AuthenticatorID) ->
    gen_server:call(?MODULE, {list_users, ChainName, AuthenticatorID}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    _ = ets:new(?CHAINS_TAB, [ named_table, set, public
                             , {keypos, #chain.name}
                             , {read_concurrency, true}]),
    {ok, #{hooked => false, providers => #{}}}.

handle_call({add_provider, AuthNType, Provider}, _From, #{providers := Providers} = State) ->
    reply(ok, State#{providers := Providers#{AuthNType => Provider}});

handle_call({create_chain, Name}, _From, State) ->
    case ets:member(?CHAINS_TAB, Name) of
        true ->
            reply({error, {already_exists, {chain, Name}}}, State);
        false ->
            Chain = #chain{name = Name,
                           authenticators = []},
            true = ets:insert(?CHAINS_TAB, Chain),
            reply({ok, serialize_chain(Chain)}, State)
    end;

handle_call({delete_chain, Name}, _From, State) ->
    case ets:lookup(?CHAINS_TAB, Name) of
        [] ->
            reply({error, {not_found, {chain, Name}}}, State);
        [#chain{authenticators = Authenticators}] ->
            _ = [do_delete_authenticator(Authenticator) || Authenticator <- Authenticators],
            true = ets:delete(?CHAINS_TAB, Name),
            reply(ok, may_unhook(State))
    end;

handle_call({lookup_chain, Name}, _From, State) ->
    case ets:lookup(?CHAINS_TAB, Name) of
        [] ->
            reply({error, {not_found, {chain, Name}}}, State);
        [Chain] ->
            reply({ok, serialize_chain(Chain)}, State)
    end;

handle_call({create_authenticator, ChainName, #{name := Name} = Config}, _From, #{providers := Providers} = State) ->
    UpdateFun = 
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case lists:keymember(Name, #authenticator.name, Authenticators) of
                false ->
                    {error, name_has_be_used};
                true ->
                    AlreadyExist = fun(ID) ->
                                       lists:keymember(ID, #authenticator.id, Authenticators)
                                   end,
                    AuthenticatorID = gen_id(AlreadyExist),
                    case do_create_authenticator(ChainName, AuthenticatorID, Config, Providers) of
                        {ok, Authenticator} ->
                            NAuthenticators = Authenticators ++ [Authenticator],
                            true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NAuthenticators}),
                            {ok, serialize_authenticator(Authenticator)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
        end,
    Reply = update_chain(ChainName, UpdateFun),
    reply(Reply, may_hook(State));

handle_call({delete_authenticator, ChainName, AuthenticatorID}, _From, State) ->
    UpdateFun = 
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case lists:keytake(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                {value, Authenticator, NAuthenticators} ->
                    _ = do_delete_authenticator(Authenticator),
                    true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NAuthenticators}),
                    ok
            end
        end,
    Reply = update_chain(ChainName, UpdateFun),
    reply(Reply, may_unhook(State));

handle_call({update_authenticator, ChainName, AuthenticatorID, Config}, _From, #{providers := Providers} = State) ->
    Reply = update_or_create_authenticator(ChainName, AuthenticatorID, Config, Providers, false),
    reply(Reply, State);

handle_call({update_or_create_authenticator, ChainName, AuthenticatorID, Config}, _From, #{providers := Providers} = State) ->
    Reply = update_or_create_authenticator(ChainName, AuthenticatorID, Config, Providers, true),
    reply(Reply, may_hook(State));

handle_call({move_authenticator, ChainName, AuthenticatorID, Position}, _From, State) ->
    UpdateFun = 
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case do_move_authenticator(AuthenticatorID, Authenticators, Position) of
                {ok, NAuthenticators} ->
                    true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NAuthenticators}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
        end,
    Reply = update_chain(ChainName, UpdateFun),
    reply(Reply, State);

handle_call({import_users, ChainName, AuthenticatorID, Filename}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, import_users, [Filename]),
    reply(Reply, State);

handle_call({add_user, ChainName, AuthenticatorID, UserInfo}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, add_user, [UserInfo]),
    reply(Reply, State);

handle_call({delete_user, ChainName, AuthenticatorID, UserID}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, delete_user, [UserID]),
    reply(Reply, State);

handle_call({update_user, ChainName, AuthenticatorID, UserID, NewUserInfo}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, update_user, [UserID, NewUserInfo]),
    reply(Reply, State);

handle_call({lookup_user, ChainName, AuthenticatorID, UserID}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, lookup_user, [UserID]),
    reply(Reply, State);

handle_call({list_users, ChainName, AuthenticatorID}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, list_users, []),
    reply(Reply, State);

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Req, State) ->
    ?LOG(error, "Unexpected case: ~p", [Req]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, State) ->
    {reply, Reply, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

may_hook(#{hooked := false} = State) ->
    case lists:any(fun(#chain{authenticators = []}) -> false;
                      (_) -> true
                   end, ets:tab2list(?CHAINS_TAB)) of
        true ->
            _ = emqx:hook('client.authenticate', {emqx_authentication, authenticate, []}),
            State#{hooked => true};
        false ->
            State
    end;
may_hook(State) ->
    State.

may_unhook(#{hooked := true} = State) ->
    case lists:all(fun(#chain{authenticators = []}) -> true;
                      (_) -> false
                   end, ets:tab2list(?CHAINS_TAB)) of
        true ->
            _ = emqx:unhook('client.authenticate', {emqx_authentication, authenticate, []}),
            State#{hooked => false};
        false ->
            State
    end;
may_unhook(State) ->
    State.

% authenticator_provider(#{mechanism := 'password-based', server_type := 'built-in-database'}) ->
%     emqx_authn_mnesia;
% authenticator_provider(#{mechanism := 'password-based', server_type := 'mysql'}) ->
%     emqx_authn_mysql;
% authenticator_provider(#{mechanism := 'password-based', server_type := 'pgsql'}) ->
%     emqx_authn_pgsql;
% authenticator_provider(#{mechanism := 'password-based', server_type := 'mongodb'}) ->
%     emqx_authn_mongodb;
% authenticator_provider(#{mechanism := 'password-based', server_type := 'redis'}) ->
%     emqx_authn_redis;
% authenticator_provider(#{mechanism := 'password-based', server_type := 'http-server'}) ->
%     emqx_authn_http;
% authenticator_provider(#{mechanism := jwt}) ->
%     emqx_authn_jwt;
% authenticator_provider(#{mechanism := scram, server_type := 'built-in-database'}) ->
%     emqx_enhanced_authn_scram_mnesia.

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

% split_by_name(Name, Config) ->
%     {Part1, Part2, true} = lists:foldl(
%              fun(#{<<"name">> := N} = C, {P1, P2, F0}) ->
%                  F = case N =:= Name of
%                          true -> true;
%                          false -> F0
%                      end,
%                  case F of
%                      false -> {[C | P1], P2, F};
%                      true -> {P1, [C | P2], F}
%                  end
%              end, {[], [], false}, Config),
%     [Found | NPart2] = lists:reverse(Part2),
%     {ok, Found, lists:reverse(Part1), NPart2}.

do_create_authenticator(ChainName, AuthenticatorID, #{name := Name, type := Type} = Config, Providers) ->
    case maps:get(Type, Providers, undefined) of
        undefined ->
            {error, no_available_provider};
        Provider ->
            Unique = <<ChainName/binary, "/", AuthenticatorID/binary, ":", ?VER_1/binary>>,
            case Provider:create(Config#{'_unique' => Unique}) of
                {ok, State} ->
                    Authenticator = #authenticator{id = AuthenticatorID,
                                                   name = Name,
                                                   provider = Provider,
                                                   state = switch_version(State)},
                    {ok, Authenticator};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

do_delete_authenticator(#authenticator{provider = Provider, state = State}) ->
    _ = Provider:destroy(State),
    ok.

update_or_create_authenticator(ChainName, AuthenticatorID, #{name := NewName, type := Type} = Config, Providers, CreateWhenNotFound) ->
    UpdateFun = 
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case lists:keytake(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    case CreateWhenNotFound of
                        true ->
                            case lists:keymember(NewName, #authenticator.name, Authenticators) of
                                true ->
                                    {error, name_has_be_used};
                                false ->
                                    case do_create_authenticator(ChainName, AuthenticatorID, Config, Providers) of
                                        {ok, Authenticator} ->
                                            NAuthenticators = Authenticators ++ [{AuthenticatorID, NewName, Authenticator}],
                                            true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NAuthenticators}),
                                            {ok, serialize_authenticator(Authenticator)};
                                        {error, Reason} ->
                                            {error, Reason}
                                    end
                            end;
                        false ->
                            {error, {not_found, {authenticator, AuthenticatorID}}}
                    end;
                {value,
                 #authenticator{provider = Provider,
                                state    = #{version := Version} = State} = Authenticator,
                 Others} ->
                    case lists:keymember(NewName, #authenticator.name, Others) of
                        true ->
                            {error, name_has_be_used};
                        false ->
                            case maps:get(Type, Providers, undefined) of
                                undefined ->
                                    {error, no_available_provider};
                                Provider ->
                                    Unique = <<ChainName/binary, "/", AuthenticatorID/binary, ":", Version/binary>>,
                                    case Provider:update(Config#{'_unique' => Unique}, State) of
                                        {ok, NewState} ->
                                            NewAuthenticator = Authenticator#authenticator{name = NewName,
                                                                                           state = switch_version(NewState)},
                                            NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                            true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NewAuthenticators}),
                                            {ok, serialize_authenticator(NewAuthenticator)};
                                        {error, Reason} ->
                                            {error, Reason}
                                    end;
                                NewProvider ->
                                    Unique = <<ChainName/binary, "/", AuthenticatorID/binary, ":", Version/binary>>,
                                    case NewProvider:create(Config#{'_unique' => Unique}) of
                                        {ok, NewState} ->
                                            NewAuthenticator = Authenticator#authenticator{name = NewName,
                                                                                           provider = NewProvider,
                                                                                           state = switch_version(NewState)},
                                            NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                            true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NewAuthenticators}),
                                            _ = Provider:destroy(State),
                                            {ok, serialize_authenticator(NewAuthenticator)};
                                        {error, Reason} ->
                                            {error, Reason}
                                    end
                            end
                    end
            end
        end,
    update_chain(ChainName, UpdateFun).
    
replace_authenticator(ID, Authenticator, Authenticators) ->
    lists:keyreplace(ID, #authenticator.id, Authenticators, Authenticator).

do_move_authenticator(ID, Authenticators, Position) ->
    case lists:keytake(ID, #authenticator.id, Authenticators) of
        false ->
            {error, {not_found, {authenticator, ID}}};
        {value, Authenticator, NAuthenticators} ->
            case Position of
                top ->
                    {ok, [Authenticator | NAuthenticators]};
                bottom ->
                    {ok, NAuthenticators ++ [Authenticator]};
                {before, ID0} ->
                    insert(Authenticator, NAuthenticators, ID0, [])
            end
    end.

insert(_, [], ID, _) ->
    {error, {not_found, {authenticator, ID}}};
insert(Authenticator, [#authenticator{id = ID} | _] = Authenticators, ID, Acc) ->
    {ok, lists:reverse(Acc) ++ [Authenticator | Authenticators]};
insert(Authenticator, [Authenticator0 | More], ID, Acc) ->
    insert(Authenticator, More, ID, [Authenticator0 | Acc]).

update_chain(ChainName, UpdateFun) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [Chain] ->
            UpdateFun(Chain)
    end.

call_authenticator(ChainName, AuthenticatorID, Func, Args) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators}) ->
            case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                #authenticator{provider = Provider, state = State} ->
                    case erlang:function_exported(Provider, Func, length(Args) + 1) of
                        true ->
                            erlang:apply(Provider, Func, Args ++ [State]);
                        false ->
                            {error, unsupported_feature}
                    end
            end
        end,
    update_chain(ChainName, UpdateFun).

serialize_chain(#chain{name = Name,
                       authenticators = Authenticators}) ->
    #{ name => Name
     , authenticators => serialize_authenticators(Authenticators)
     }.

serialize_authenticators(Authenticators) ->
    [serialize_authenticator(Authenticator) || {_, _, Authenticator} <- Authenticators].

serialize_authenticator(#authenticator{id = ID,
                                       name = Name,
                                       provider = Provider,
                                       state = State}) ->
    #{ id => ID
     , name => Name
     , provider => Provider
     , state => State
     }.
