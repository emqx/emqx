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

-behaviour(gen_server).

-behaviour(emqx_config_handler).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ pre_config_update/2
        , post_config_update/3
        , update_config/2
        ]).

-export([ enable/0
        , disable/0
        , is_enabled/0
        ]).

-export([authenticate/2]).

-export([ start_link/0
        , stop/0
        ]).

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

-define(CHAIN_TAB, emqx_authn_chain).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

pre_config_update({enable, Enable}, _OldConfig) ->
    {ok, Enable};
pre_config_update({create_authenticator, Config}, OldConfig) ->
    {ok, OldConfig ++ [Config]};
pre_config_update({delete_authenticator, ID}, OldConfig) ->
    case lookup_authenticator(?CHAIN, ID) of
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            NewConfig = lists:filter(fun(#{<<"name">> := N}) ->
                                         N =/= Name
                                     end, OldConfig),
            {ok, NewConfig}
    end;
pre_config_update({update_authenticator, ID, Config}, OldConfig) ->
    case lookup_authenticator(?CHAIN, ID) of
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            NewConfig = lists:map(fun(#{<<"name">> := N} = C) ->
                                      case N =:= Name of
                                          true -> Config;
                                          false -> C
                                      end
                                  end, OldConfig),
            {ok, NewConfig}
    end;
pre_config_update({update_or_create_authenticator, ID, Config}, OldConfig) ->
    case lookup_authenticator(?CHAIN, ID) of
        {error, _Reason} -> OldConfig ++ [Config];
        {ok, #{name := Name}} ->
            NewConfig = lists:map(fun(#{<<"name">> := N} = C) ->
                                      case N =:= Name of
                                          true -> Config;
                                          false -> C
                                      end
                                  end, OldConfig),
            {ok, NewConfig}
    end;
pre_config_update({move_authenticator, ID, Position}, OldConfig) ->
    case lookup_authenticator(?CHAIN, ID) of
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            {ok, Found, Part1, Part2} = split_by_name(Name, OldConfig),
            case Position of
                <<"top">> ->
                    {ok, [Found | Part1] ++ Part2};
                <<"bottom">> ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                Before ->
                    case binary:split(Before, <<":">>, [global]) of
                        [<<"before">>, ID0] ->
                            case lookup_authenticator(?CHAIN, ID0) of
                                {error, Reason} -> {error, Reason};
                                {ok, #{name := Name1}} ->
                                    {ok, NFound, NPart1, NPart2} = split_by_name(Name1, Part1 ++ Part2),
                                    {ok, NPart1 ++ [Found, NFound | NPart2]}
                            end;
                        _ ->
                            {error, {invalid_parameter, position}}
                    end
            end
    end.

post_config_update({enable, true}, _NewConfig, _OldConfig) ->
    emqx_authn:enable();
post_config_update({enable, false}, _NewConfig, _OldConfig) ->
    emqx_authn:disable();
post_config_update({create_authenticator, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            create_authenticator(?CHAIN, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
post_config_update({delete_authenticator, ID}, _NewConfig, _OldConfig) ->
    case delete_authenticator(?CHAIN, ID) of
        ok -> ok;
        {error, Reason} -> throw(Reason)
    end;
post_config_update({update_authenticator, ID, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            update_authenticator(?CHAIN, ID, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
post_config_update({update_or_create_authenticator, ID, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            update_or_create_authenticator(?CHAIN, ID, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
post_config_update({move_authenticator, ID, Position}, _NewConfig, _OldConfig) ->
    NPosition = case Position of
                    <<"top">> -> top;
                    <<"bottom">> -> bottom;
                    Before ->
                        case binary:split(Before, <<":">>, [global]) of
                            [<<"before">>, ID0] ->
                                {before, ID0};
                            _ ->
                                {error, {invalid_parameter, position}}
                        end
                end,
    move_authenticator(?CHAIN, ID, NPosition).

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

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
    case ets:lookup(?CHAIN_TAB, ?CHAIN) of
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

create_chain(#{id := ID}) ->
    gen_server:call(?MODULE, {create_chain, ID}).

delete_chain(ID) ->
    gen_server:call(?MODULE, {delete_chain, ID}).

lookup_chain(ID) ->
    gen_server:call(?MODULE, {lookup_chain, ID}).

list_chains() ->
    Chains = ets:tab2list(?CHAIN_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

create_authenticator(ChainID, Config) ->
    gen_server:call(?MODULE, {create_authenticator, ChainID, Config}).

delete_authenticator(ChainID, AuthenticatorID) ->
    gen_server:call(?MODULE, {delete_authenticator, ChainID, AuthenticatorID}).

update_authenticator(ChainID, AuthenticatorID, Config) ->
    gen_server:call(?MODULE, {update_authenticator, ChainID, AuthenticatorID, Config}).

update_or_create_authenticator(ChainID, AuthenticatorID, Config) ->
    gen_server:call(?MODULE, {update_or_create_authenticator, ChainID, AuthenticatorID, Config}).

lookup_authenticator(ChainID, AuthenticatorID) ->
    case ets:lookup(?CHAIN_TAB, ChainID) of
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
    case ets:lookup(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{authenticators = Authenticators}] ->
            {ok, serialize_authenticators(Authenticators)}
    end.

move_authenticator(ChainID, AuthenticatorID, Position) ->
    gen_server:call(?MODULE, {move_authenticator, ChainID, AuthenticatorID, Position}).

import_users(ChainID, AuthenticatorID, Filename) ->
    gen_server:call(?MODULE, {import_users, ChainID, AuthenticatorID, Filename}).

add_user(ChainID, AuthenticatorID, UserInfo) ->
    gen_server:call(?MODULE, {add_user, ChainID, AuthenticatorID, UserInfo}).

delete_user(ChainID, AuthenticatorID, UserID) ->
    gen_server:call(?MODULE, {delete_user, ChainID, AuthenticatorID, UserID}).

update_user(ChainID, AuthenticatorID, UserID, NewUserInfo) ->
    gen_server:call(?MODULE, {update_user, ChainID, AuthenticatorID, UserID, NewUserInfo}).

lookup_user(ChainID, AuthenticatorID, UserID) ->
    gen_server:call(?MODULE, {lookup_user, ChainID, AuthenticatorID, UserID}).

%% TODO: Support pagination
list_users(ChainID, AuthenticatorID) ->
    gen_server:call(?MODULE, {list_users, ChainID, AuthenticatorID}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    _ = ets:new(?CHAIN_TAB, [ named_table, set, public
                            , {keypos, #chain.id}
                            , {read_concurrency, true}]),
    {ok, #{}}.

handle_call({create_chain, ID}, _From, State) ->
    case ets:member(?CHAIN_TAB, ID) of
        true ->
            reply({error, {already_exists, {chain, ID}}}, State);
        false ->
            Chain = #chain{id = ID,
                           authenticators = [],
                           created_at = erlang:system_time(millisecond)},
            true = ets:insert(?CHAIN_TAB, Chain),
            reply({ok, serialize_chain(Chain)}, State)
    end;

handle_call({delete_chain, ID}, _From, State) ->
    case ets:lookup(?CHAIN_TAB, ID) of
        [] ->
            reply({error, {not_found, {chain, ID}}}, State);
        [#chain{authenticators = Authenticators}] ->
            _ = [do_delete_authenticator(Authenticator) || {_, _, Authenticator} <- Authenticators],
            true = ets:delete(?CHAIN_TAB, ID),
            reply(ok, State)
    end;

handle_call({lookup_chain, ID}, _From, State) ->
    case ets:lookup(?CHAIN_TAB, ID) of
        [] ->
            reply({error, {not_found, {chain, ID}}}, State);
        [Chain] ->
            reply({ok, serialize_chain(Chain)}, State)
    end;

handle_call({create_authenticator, ChainID, #{name := Name} = Config}, _From, State) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators} = Chain) ->
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
                            true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}),
                            {ok, serialize_authenticator(Authenticator)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
        end,
    Reply = update_chain(ChainID, UpdateFun),
    reply(Reply, State);

handle_call({delete_authenticator, ChainID, AuthenticatorID}, _From, State) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case lists:keytake(AuthenticatorID, 1, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                {value, {_, _, Authenticator}, NAuthenticators} ->
                    _ = do_delete_authenticator(Authenticator),
                    true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}),
                    ok
            end
        end,
    Reply = update_chain(ChainID, UpdateFun),
    reply(Reply, State);

handle_call({update_authenticator, ChainID, AuthenticatorID, Config}, _From, State) ->
    Reply = update_or_create_authenticator(ChainID, AuthenticatorID, Config, false),
    reply(Reply, State);

handle_call({update_or_create_authenticator, ChainID, AuthenticatorID, Config}, _From, State) ->
    Reply = update_or_create_authenticator(ChainID, AuthenticatorID, Config, true),
    reply(Reply, State);

handle_call({move_authenticator, ChainID, AuthenticatorID, Position}, _From, State) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case do_move_authenticator(AuthenticatorID, Authenticators, Position) of
                {ok, NAuthenticators} ->
                    true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
        end,
    Reply = update_chain(ChainID, UpdateFun),
    reply(Reply, State);

handle_call({import_users, ChainID, AuthenticatorID, Filename}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, import_users, [Filename]),
    reply(Reply, State);

handle_call({add_user, ChainID, AuthenticatorID, UserInfo}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, add_user, [UserInfo]),
    reply(Reply, State);

handle_call({delete_user, ChainID, AuthenticatorID, UserID}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, delete_user, [UserID]),
    reply(Reply, State);

handle_call({update_user, ChainID, AuthenticatorID, UserID, NewUserInfo}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, update_user, [UserID, NewUserInfo]),
    reply(Reply, State);

handle_call({lookup_user, ChainID, AuthenticatorID, UserID}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, lookup_user, [UserID]),
    reply(Reply, State);

handle_call({list_users, ChainID, AuthenticatorID}, _From, State) ->
    Reply = call_authenticator(ChainID, AuthenticatorID, list_users, []),
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

split_by_name(Name, Config) ->
    {Part1, Part2, true} = lists:foldl(
             fun(#{<<"name">> := N} = C, {P1, P2, F0}) ->
                 F = case N =:= Name of
                         true -> true;
                         false -> F0
                     end,
                 case F of
                     false -> {[C | P1], P2, F};
                     true -> {P1, [C | P2], F}
                 end
             end, {[], [], false}, Config),
    [Found | NPart2] = lists:reverse(Part2),
    {ok, Found, lists:reverse(Part1), NPart2}.

do_create_authenticator(ChainID, AuthenticatorID, #{name := Name} = Config) ->
    Provider = authenticator_provider(Config),
    Unique = <<ChainID/binary, "/", AuthenticatorID/binary, ":", ?VER_1/binary>>,
    case Provider:create(Config#{'_unique' => Unique}) of
        {ok, State} ->
            Authenticator = #authenticator{id = AuthenticatorID,
                                           name = Name,
                                           provider = Provider,
                                           state = switch_version(State)},
            {ok, Authenticator};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete_authenticator(#authenticator{provider = Provider, state = State}) ->
    _ = Provider:destroy(State),
    ok.

update_or_create_authenticator(ChainID, AuthenticatorID, #{name := NewName} = Config, CreateWhenNotFound) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators} = Chain) ->
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
                                            true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NAuthenticators}),
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
                                                                                           state = switch_version(NewState)},
                                            NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                            true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NewAuthenticators}),
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
                                                                                           state = switch_version(NewState)},
                                            NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                            true = ets:insert(?CHAIN_TAB, Chain#chain{authenticators = NewAuthenticators}),
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

replace_authenticator(ID, #authenticator{name = Name} = Authenticator, Authenticators) ->
    lists:keyreplace(ID, 1, Authenticators, {ID, Name, Authenticator}).

do_move_authenticator(AuthenticatorID, Authenticators, Position) when is_binary(AuthenticatorID) ->
    case lists:keytake(AuthenticatorID, 1, Authenticators) of
        false ->
            {error, {not_found, {authenticator, AuthenticatorID}}};
        {value, Authenticator, NAuthenticators} ->
            do_move_authenticator(Authenticator, NAuthenticators, Position)
    end;

do_move_authenticator(Authenticator, Authenticators, top) ->
    {ok, [Authenticator | Authenticators]};
do_move_authenticator(Authenticator, Authenticators, bottom) ->
    {ok, Authenticators ++ [Authenticator]};
do_move_authenticator(Authenticator, Authenticators, {before, ID}) ->
    insert(Authenticator, Authenticators, ID, []).

insert(_, [], ID, _) ->
    {error, {not_found, {authenticator, ID}}};
insert(Authenticator, [{ID, _, _} | _] = Authenticators, ID, Acc) ->
    {ok, lists:reverse(Acc) ++ [Authenticator | Authenticators]};
insert(Authenticator, [{_, _, _} = Authenticator0 | More], ID, Acc) ->
    insert(Authenticator, More, ID, [Authenticator0 | Acc]).

update_chain(ChainID, UpdateFun) ->
    case ets:lookup(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [Chain] ->
            UpdateFun(Chain)
    end.

call_authenticator(ChainID, AuthenticatorID, Func, Args) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators}) ->
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
        end,
    update_chain(ChainID, UpdateFun).

serialize_chain(#chain{id = ID,
                       authenticators = Authenticators,
                       created_at = CreatedAt}) ->
    #{id => ID,
      authenticators => serialize_authenticators(Authenticators),
      created_at => CreatedAt}.

serialize_authenticators(Authenticators) ->
    [serialize_authenticator(Authenticator) || {_, _, Authenticator} <- Authenticators].

serialize_authenticator(#authenticator{id = ID,
                                       name = Name,
                                       provider = Provider,
                                       state = State}) ->
    #{id => ID, name => Name, provider => Provider, state => State}.
