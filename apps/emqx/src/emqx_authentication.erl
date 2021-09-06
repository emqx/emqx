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
-behaviour(hocon_schema).
-behaviour(emqx_config_handler).

-include("emqx.hrl").
-include("logger.hrl").

-export([ roots/0
        , fields/1
        ]).

-export([ pre_config_update/2
        , post_config_update/4
        ]).

-export([ authenticate/2
        ]).

-export([ initialize_authentication/2 ]).

-export([ start_link/0
        , stop/0
        ]).

-export([ add_provider/2
        , remove_provider/1
        , create_chain/1
        , delete_chain/1
        , lookup_chain/1
        , list_chains/0
        , create_authenticator/2
        , delete_authenticator/2
        , update_authenticator/3
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

-export([ generate_id/1 ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(CHAINS_TAB, emqx_authn_chains).

-define(VER_1, <<"1">>).
-define(VER_2, <<"2">>).

-type config() :: #{atom() => term()}.
-type state() :: #{atom() => term()}.
-type extra() :: #{superuser := boolean(),
                   atom() => term()}.
-type user_info() :: #{user_id := binary(),
                       atom() => term()}.

-callback refs() -> [{ref, Module, Name}] when Module::module(), Name::atom().

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
%% Hocon Schema
%%------------------------------------------------------------------------------

roots() -> [{authentication, fun authentication/1}].

fields(_) -> [].

authentication(type) ->
    {ok, Refs} = get_refs(),
    hoconsc:union([hoconsc:array(hoconsc:union(Refs)) | Refs]);
authentication(default) -> [];
authentication(_) -> undefined.

%%------------------------------------------------------------------------------
%% Callbacks of config handler
%%------------------------------------------------------------------------------

pre_config_update(UpdateReq, OldConfig) ->
    case do_pre_config_update(UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, may_to_map(NewConfig)}
    end.

do_pre_config_update({create_authenticator, _ChainName, Config}, OldConfig) ->
    {ok, OldConfig ++ [Config]};
do_pre_config_update({delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(fun(OldConfig0) ->
                                AuthenticatorID =/= generate_id(OldConfig0)
                             end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({update_authenticator, _ChainName, AuthenticatorID, Config}, OldConfig) ->
    NewConfig = lists:map(fun(OldConfig0) ->
                              case AuthenticatorID =:= generate_id(OldConfig0) of
                                  true -> maps:merge(OldConfig0, Config);
                                  false -> OldConfig0
                              end
                          end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} -> {error, Reason};
        {ok, Part1, [Found | Part2]} ->
            case Position of
                <<"top">> ->
                    {ok, [Found | Part1] ++ Part2};
                <<"bottom">> ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                <<"before:", Before/binary>> ->
                    case split_by_id(Before, Part1 ++ Part2) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NPart1, [NFound | NPart2]} ->
                            {ok, NPart1 ++ [Found, NFound | NPart2]}
                    end;
                _ ->
                    {error, {invalid_parameter, position}}
            end
    end.

post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_config(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    _ = create_chain(ChainName),
    create_authenticator(ChainName, NConfig);

do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, _OldConfig, _AppEnvs) ->
    delete_authenticator(ChainName, AuthenticatorID);

do_post_config_update({update_authenticator, ChainName, AuthenticatorID, _Config}, NewConfig, _OldConfig, _AppEnvs) ->
    [Config] = lists:filter(fun(NewConfig0) ->
                                AuthenticatorID =:= generate_id(NewConfig0)
                            end, NewConfig),
    NConfig = check_config(Config),
    update_authenticator(ChainName, AuthenticatorID, NConfig);

do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    NPosition = case Position of
                    <<"top">> -> top;
                    <<"bottom">> -> bottom;
                    <<"before:", Before/binary>> ->
                        {before, Before}
                end,
    move_authenticator(ChainName, AuthenticatorID, NPosition).

check_config(Config) ->
    #{authentication := CheckedConfig} = hocon_schema:check_plain(emqx_authentication,
        #{<<"authentication">> => Config}, #{nullable => true, atom_key => true}),
    CheckedConfig.

%%------------------------------------------------------------------------------
%% Authenticate
%%------------------------------------------------------------------------------

authenticate(#{listener := Listener, protocol := Protocol} = Credential, _AuthResult) ->
    case ets:lookup(?CHAINS_TAB, Listener) of
        [#chain{authenticators = Authenticators}] when Authenticators =/= [] ->
            do_authenticate(Authenticators, Credential);
        _ ->
            case ets:lookup(?CHAINS_TAB, global_chain(Protocol)) of
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
            %% {continue, AuthCache}
            %% {continue, AuthData, AuthCache}
            %% {error, Reason}
            {stop, Result}
    end.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

initialize_authentication(_, []) ->
    ok;
initialize_authentication(ChainName, AuthenticatorsConfig) ->
    _ = create_chain(ChainName),
    CheckedConfig = check_config(to_list(AuthenticatorsConfig)),
    lists:foreach(fun(AuthenticatorConfig) ->
        case create_authenticator(ChainName, AuthenticatorConfig) of
            {ok, _} ->
                ok;
            {error, Reason} ->
                ?LOG(error, "Failed to create authenticator '~s': ~p", [generate_id(AuthenticatorConfig), Reason])
        end
    end, CheckedConfig).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

get_refs() ->
    gen_server:call(?MODULE, get_refs).

add_provider(AuthNType, Provider) ->
    gen_server:call(?MODULE, {add_provider, AuthNType, Provider}).

remove_provider(AuthNType) ->
    gen_server:call(?MODULE, {remove_provider, AuthNType}).

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

lookup_authenticator(ChainName, AuthenticatorID) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [#chain{authenticators = Authenticators}] ->
            case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                Authenticator ->
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

generate_id(#{mechanism := Mechanism0, backend := Backend0}) ->
    Mechanism = atom_to_binary(Mechanism0),
    Backend = atom_to_binary(Backend0),
    <<Mechanism/binary, ":", Backend/binary>>;
generate_id(#{mechanism := Mechanism}) ->
    atom_to_binary(Mechanism);
generate_id(#{<<"mechanism">> := Mechanism, <<"backend">> := Backend}) ->
    <<Mechanism/binary, ":", Backend/binary>>;
generate_id(#{<<"mechanism">> := Mechanism}) ->
    Mechanism.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    _ = ets:new(?CHAINS_TAB, [ named_table, set, public
                             , {keypos, #chain.name}
                             , {read_concurrency, true}]),
    ok = emqx_config_handler:add_handler([authentication], ?MODULE),
    ok = emqx_config_handler:add_handler([listeners, '?', '?', authentication], ?MODULE),
    {ok, #{hooked => false, providers => #{}}}.

handle_call({add_provider, AuthNType, Provider}, _From, #{providers := Providers} = State) ->
    reply(ok, State#{providers := Providers#{AuthNType => Provider}});

handle_call({remove_provider, AuthNType}, _From, #{providers := Providers} = State) ->
    reply(ok, State#{providers := maps:remove(AuthNType, Providers)});

handle_call(get_refs, _From, #{providers := Providers} = State) ->
    Refs = lists:foldl(fun({_, Provider}, Acc) ->
                           Acc ++ Provider:refs()
                       end, [], maps:to_list(Providers)),
    reply({ok, Refs}, State);

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

handle_call({create_authenticator, ChainName, Config}, _From, #{providers := Providers} = State) ->
    UpdateFun = 
        fun(#chain{authenticators = Authenticators} = Chain) ->
            AuthenticatorID = generate_id(Config),
            case lists:keymember(AuthenticatorID, #authenticator.id, Authenticators) of
                true ->
                    {error, {already_exists, {authenticator, AuthenticatorID}}};
                false ->
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

handle_call({update_authenticator, ChainName, AuthenticatorID, Config}, _From, State) ->
    UpdateFun =
        fun(#chain{authenticators = Authenticators} = Chain) ->
            case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                #authenticator{provider = Provider,
                               state    = #{version := Version} = ST} = Authenticator ->
                    case AuthenticatorID =:= generate_id(Config) of
                        true ->
                            Unique = <<ChainName/binary, "/", AuthenticatorID/binary, ":", Version/binary>>,
                            case Provider:update(Config#{'_unique' => Unique}, ST) of
                                {ok, NewST} ->
                                    NewAuthenticator = Authenticator#authenticator{state = switch_version(NewST)},
                                    NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                    true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NewAuthenticators}),
                                    {ok, serialize_authenticator(NewAuthenticator)};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        false ->
                            {error, mechanism_or_backend_change_is_not_alloed}
                    end
            end
        end,
    Reply = update_chain(ChainName, UpdateFun),
    reply(Reply, State);

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
    emqx_config_handler:remove_handler([authentication]),
    emqx_config_handler:remove_handler([listeners, '?', '?', authentication]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, State) ->
    {reply, Reply, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

split_by_id(ID, AuthenticatorsConfig) ->
    case lists:foldl(
             fun(C, {P1, P2, F0}) ->
                 F = case ID =:= generate_id(C) of
                         true -> true;
                         false -> F0
                     end,
                 case F of
                     false -> {[C | P1], P2, F};
                     true -> {P1, [C | P2], F}
                 end
             end, {[], [], false}, AuthenticatorsConfig) of
        {_, _, false} ->
            {error, {not_found, {authenticator, ID}}};
        {Part1, Part2, true} ->
            {ok, lists:reverse(Part1), lists:reverse(Part2)}
    end.

global_chain(mqtt) ->
    <<"mqtt:global">>;
global_chain('mqtt-sn') ->
    <<"mqtt-sn:global">>;
global_chain(coap) ->
    <<"coap:global">>;
global_chain(lwm2m) ->
    <<"lwm2m:global">>;
global_chain(stomp) ->
    <<"stomp:global">>;
global_chain(_) ->
    <<"unknown:global">>.

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

do_create_authenticator(ChainName, AuthenticatorID, Config, Providers) ->
    case maps:get(authn_type(Config), Providers, undefined) of
        undefined ->
            {error, no_available_provider};
        Provider ->
            Unique = <<ChainName/binary, "/", AuthenticatorID/binary, ":", ?VER_1/binary>>,
            case Provider:create(Config#{'_unique' => Unique}) of
                {ok, State} ->
                    Authenticator = #authenticator{id = AuthenticatorID,
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
    [serialize_authenticator(Authenticator) || Authenticator <- Authenticators].

serialize_authenticator(#authenticator{id = ID,
                                       provider = Provider,
                                       state = State}) ->
    #{ id => ID
     , provider => Provider
     , state => State
     }.

switch_version(State = #{version := ?VER_1}) ->
    State#{version := ?VER_2};
switch_version(State = #{version := ?VER_2}) ->
    State#{version := ?VER_1};
switch_version(State) ->
    State#{version => ?VER_1}.

authn_type(#{mechanism := Mechanism, backend := Backend}) ->
    {Mechanism, Backend};
authn_type(#{mechanism := Mechanism}) ->
    Mechanism.

may_to_map([L]) ->
    L;
may_to_map(L) ->
    L.

to_list(undefined) ->
    [];
to_list(M) when M =:= #{} ->
    [];
to_list(M) when is_map(M) ->
    [M];
to_list(L) when is_list(L) ->
    L.
