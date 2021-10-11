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

-export([ register_provider/2
        , register_providers/1
        , deregister_provider/1
        , deregister_providers/1
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

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(CHAINS_TAB, emqx_authn_chains).

-define(VER_1, <<"1">>).
-define(VER_2, <<"2">>).

-type chain_name() :: atom().
-type authenticator_id() :: binary().
-type position() :: top | bottom | {before, authenticator_id()}.
-type update_request() :: {create_authenticator, chain_name(), map()}
                        | {delete_authenticator, chain_name(), authenticator_id()}
                        | {update_authenticator, chain_name(), authenticator_id(), map()}
                        | {move_authenticator, chain_name(), authenticator_id(), position()}.
-type authn_type() :: atom() | {atom(), atom()}.
-type provider() :: module().

-type chain() :: #{name := chain_name(),
                   authenticators := [authenticator()]}.

-type authenticator() :: #{id := authenticator_id(),
                           provider := provider(),
                           enable := boolean(),
                           state := map()}.


-type config() :: #{atom() => term()}.
-type state() :: #{atom() => term()}.
-type extra() :: #{is_superuser := boolean(),
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
    when UserID::binary(), UserInfo::map(), State::state(), User::user_info().

-callback lookup_user(UserID, UserInfo, State)
    -> {ok, User}
     | {error, term()}
    when UserID::binary(), UserInfo::map(), State::state(), User::user_info().

-callback list_users(State)
    -> {ok, Users}
    when State::state(), Users::[user_info()].

-optional_callbacks([ import_users/2
                    , add_user/2
                    , delete_user/2
                    , update_user/3
                    , lookup_user/3
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

-spec pre_config_update(update_request(), emqx_config:raw_config())
    -> {ok, map() | list()} | {error, term()}.
pre_config_update(UpdateReq, OldConfig) ->
    case do_pre_config_update(UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, may_to_map(NewConfig)}
    end.

do_pre_config_update({create_authenticator, ChainName, Config}, OldConfig) ->
    try 
        CertsDir = certs_dir([to_bin(ChainName), generate_id(Config)]),
        NConfig = convert_certs(CertsDir, Config),
        {ok, OldConfig ++ [NConfig]}
    catch
        error:{save_cert_to_file, _} = Reason ->
            {error, Reason};
        error:{missing_parameter, _} = Reason ->
            {error, Reason}
    end;
do_pre_config_update({delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(fun(OldConfig0) ->
                                AuthenticatorID =/= generate_id(OldConfig0)
                             end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    try 
        CertsDir = certs_dir([to_bin(ChainName), AuthenticatorID]),
        NewConfig = lists:map(
                        fun(OldConfig0) ->
                            case AuthenticatorID =:= generate_id(OldConfig0) of
                                true -> convert_certs(CertsDir, Config, OldConfig0);
                                false -> OldConfig0
                            end
                        end, OldConfig),
        {ok, NewConfig}
    catch
        error:{save_cert_to_file, _} = Reason ->
            {error, Reason};
         error:{missing_parameter, _} = Reason ->
            {error, Reason}
    end;
do_pre_config_update({move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} -> {error, Reason};
        {ok, Part1, [Found | Part2]} ->
            case Position of
                top ->
                    {ok, [Found | Part1] ++ Part2};
                bottom ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                {before, Before} ->
                    case split_by_id(Before, Part1 ++ Part2) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NPart1, [NFound | NPart2]} ->
                            {ok, NPart1 ++ [Found, NFound | NPart2]}
                    end
            end
    end.

-spec post_config_update(update_request(), map() | list(), emqx_config:raw_config(), emqx_config:app_envs())
    -> ok | {ok, map()} | {error, term()}.
post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_config(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    _ = create_chain(ChainName),
    create_authenticator(ChainName, NConfig);

do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, OldConfig, _AppEnvs) ->
    case delete_authenticator(ChainName, AuthenticatorID) of
        ok ->
            [Config] = [Config0 || Config0 <- to_list(OldConfig), AuthenticatorID == generate_id(Config0)],
            CertsDir = certs_dir([to_bin(ChainName), AuthenticatorID]),
            clear_certs(CertsDir, Config),
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

do_post_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    update_authenticator(ChainName, AuthenticatorID, NConfig);

do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    move_authenticator(ChainName, AuthenticatorID, Position).

check_config(Config) ->
    #{authentication := CheckedConfig} =
        hocon_schema:check_plain(?MODULE, #{<<"authentication">> => Config}, #{atom_key => true}),
    CheckedConfig.

%%------------------------------------------------------------------------------
%% Authenticate
%%------------------------------------------------------------------------------

authenticate(#{listener := Listener, protocol := Protocol} = Credential, _AuthResult) ->
    Authenticators = get_authenticators(Listener, global_chain(Protocol)),
    case get_enabled(Authenticators) of
        [] -> ignore;
        NAuthenticators -> do_authenticate(NAuthenticators, Credential)
    end.

do_authenticate([], _) ->
    {stop, {error, not_authorized}};
do_authenticate([#authenticator{id = ID, provider = Provider, state = State} | More], Credential) ->
    try Provider:authenticate(Credential, State) of
        ignore ->
            do_authenticate(More, Credential);
        Result ->
            %% {ok, Extra}
            %% {ok, Extra, AuthData}
            %% {continue, AuthCache}
            %% {continue, AuthData, AuthCache}
            %% {error, Reason}
            {stop, Result}
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(warning, #{msg => "unexpected_error_in_authentication",
                             exception => Class,
                             reason => Reason,
                             stacktrace => Stacktrace,
                             authenticator => ID}),
            do_authenticate(More, Credential)
    end.

get_authenticators(Listener, Global) ->
    case ets:lookup(?CHAINS_TAB, Listener) of
        [#chain{authenticators = Authenticators}] ->
            Authenticators;
        _ ->
            case ets:lookup(?CHAINS_TAB, Global) of
                [#chain{authenticators = Authenticators}] ->
                    Authenticators;
                _ ->
                    []
            end
    end.

get_enabled(Authenticators) ->
    [Authenticator || Authenticator <- Authenticators, Authenticator#authenticator.enable =:= true].

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec initialize_authentication(chain_name(), [#{binary() => term()}]) -> ok.
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
                ?SLOG(error, #{
                    msg => "failed_to_create_authenticator",
                    authenticator => generate_id(AuthenticatorConfig),
                    reason => Reason
                })
        end
    end, CheckedConfig).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

-spec get_refs() -> {ok, Refs} when Refs :: [{authn_type(), module()}].
get_refs() ->
    call(get_refs).

%% @doc Register authentication providers.
%% A provider is a tuple of `AuthNType' the module which implements
%% the authenticator callbacks.
%% For example, ``[{{'password-based', redis}, emqx_authn_redis}]''
%% NOTE: Later registered provider may override earlier registered if they
%% happen to clash the same `AuthNType'.
-spec register_providers([{authn_type(), module()}]) -> ok.
register_providers(Providers) ->
    call({register_providers, Providers}).

-spec register_provider(authn_type(), module()) -> ok.
register_provider(AuthNType, Provider) ->
    register_providers([{AuthNType, Provider}]).

-spec deregister_providers([authn_type()]) -> ok.
deregister_providers(AuthNTypes) when is_list(AuthNTypes) ->
    call({deregister_providers, AuthNTypes}).

-spec deregister_provider(authn_type()) -> ok.
deregister_provider(AuthNType) ->
    deregister_providers([AuthNType]).

-spec create_chain(chain_name()) -> {ok, chain()} | {error, term()}.
create_chain(Name) ->
    call({create_chain, Name}).

-spec delete_chain(chain_name()) -> ok | {error, term()}.
delete_chain(Name) ->
    call({delete_chain, Name}).

-spec lookup_chain(chain_name()) -> {ok, chain()} | {error, term()}.
lookup_chain(Name) ->
    call({lookup_chain, Name}).

-spec list_chains() -> {ok, [chain()]}.
list_chains() ->
    Chains = ets:tab2list(?CHAINS_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

-spec create_authenticator(chain_name(), config()) -> {ok, authenticator()} | {error, term()}.
create_authenticator(ChainName, Config) ->
    call({create_authenticator, ChainName, Config}).

-spec delete_authenticator(chain_name(), authenticator_id()) -> ok | {error, term()}.
delete_authenticator(ChainName, AuthenticatorID) ->
    call({delete_authenticator, ChainName, AuthenticatorID}).

-spec update_authenticator(chain_name(), authenticator_id(), config()) -> {ok, authenticator()} | {error, term()}.
update_authenticator(ChainName, AuthenticatorID, Config) ->
    call({update_authenticator, ChainName, AuthenticatorID, Config}).

-spec lookup_authenticator(chain_name(), authenticator_id()) -> {ok, authenticator()} | {error, term()}.
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

-spec list_authenticators(chain_name()) -> {ok, [authenticator()]} | {error, term()}.
list_authenticators(ChainName) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [#chain{authenticators = Authenticators}] ->
            {ok, serialize_authenticators(Authenticators)}
    end.

-spec move_authenticator(chain_name(), authenticator_id(), position()) -> ok | {error, term()}.
move_authenticator(ChainName, AuthenticatorID, Position) ->
    call({move_authenticator, ChainName, AuthenticatorID, Position}).

-spec import_users(chain_name(), authenticator_id(), binary()) -> ok | {error, term()}.
import_users(ChainName, AuthenticatorID, Filename) ->
    call({import_users, ChainName, AuthenticatorID, Filename}).

-spec add_user(chain_name(), authenticator_id(), user_info()) -> {ok, user_info()} | {error, term()}.
add_user(ChainName, AuthenticatorID, UserInfo) ->
    call({add_user, ChainName, AuthenticatorID, UserInfo}).

-spec delete_user(chain_name(), authenticator_id(), binary()) -> ok | {error, term()}.
delete_user(ChainName, AuthenticatorID, UserID) ->
    call({delete_user, ChainName, AuthenticatorID, UserID}).

-spec update_user(chain_name(), authenticator_id(), binary(), map()) -> {ok, user_info()} | {error, term()}.
update_user(ChainName, AuthenticatorID, UserID, NewUserInfo) ->
    call({update_user, ChainName, AuthenticatorID, UserID, NewUserInfo}).

-spec lookup_user(chain_name(), authenticator_id(), binary()) -> {ok, user_info()} | {error, term()}.
lookup_user(ChainName, AuthenticatorID, UserID) ->
    call({lookup_user, ChainName, AuthenticatorID, UserID}).

%% TODO: Support pagination
-spec list_users(chain_name(), authenticator_id()) -> {ok, [user_info()]} | {error, term()}.
list_users(ChainName, AuthenticatorID) ->
    call({list_users, ChainName, AuthenticatorID}).

-spec generate_id(config()) -> authenticator_id().
generate_id(#{mechanism := Mechanism0, backend := Backend0}) ->
    Mechanism = to_bin(Mechanism0),
    Backend = to_bin(Backend0),
    <<Mechanism/binary, ":", Backend/binary>>;
generate_id(#{mechanism := Mechanism}) ->
    to_bin(Mechanism);
generate_id(#{<<"mechanism">> := Mechanism, <<"backend">> := Backend}) ->
    <<Mechanism/binary, ":", Backend/binary>>;
generate_id(#{<<"mechanism">> := Mechanism}) ->
    Mechanism;
generate_id(_) ->
    error({missing_parameter, mechanism}).

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

handle_call({register_providers, Providers}, _From,
            #{providers := Reg0} = State) ->
    case lists:filter(fun({T, _}) -> maps:is_key(T, Reg0) end, Providers) of
        [] ->
            Reg = lists:foldl(fun({AuthNType, Module}, Pin) ->
                                      Pin#{AuthNType => Module}
                              end, Reg0, Providers),
            reply(ok, State#{providers := Reg});
        Clashes ->
            reply({error, {authentication_type_clash, Clashes}}, State)
    end;

handle_call({deregister_providers, AuthNTypes}, _From, #{providers := Providers} = State) ->
    reply(ok, State#{providers := maps:without(AuthNTypes, Providers)});

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
            reply(ok, maybe_unhook(State))
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
                            NAuthenticators = Authenticators ++ [Authenticator#authenticator{enable = maps:get(enable, Config)}],
                            true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NAuthenticators}),
                            {ok, serialize_authenticator(Authenticator)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
        end,
    Reply = update_chain(ChainName, UpdateFun),
    reply(Reply, maybe_hook(State));

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
    reply(Reply, maybe_unhook(State));

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
                            Unique = unique(ChainName, AuthenticatorID, Version),
                            case Provider:update(Config#{'_unique' => Unique}, ST) of
                                {ok, NewST} ->
                                    NewAuthenticator = Authenticator#authenticator{state = switch_version(NewST),
                                                                                   enable = maps:get(enable, Config)},
                                    NewAuthenticators = replace_authenticator(AuthenticatorID, NewAuthenticator, Authenticators),
                                    true = ets:insert(?CHAINS_TAB, Chain#chain{authenticators = NewAuthenticators}),
                                    {ok, serialize_authenticator(NewAuthenticator)};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        false ->
                            {error, change_of_authentication_type_is_not_allowed}
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
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
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

certs_dir(Dirs) when is_list(Dirs) ->
    to_bin(filename:join([emqx:get_config([node, data_dir]), "certs/authn"] ++ Dirs)).

convert_certs(CertsDir, Config) ->
    case maps:get(<<"ssl">>, Config, undefined) of
        undefined ->
            Config;
        SSLOpts ->
            NSSLOPts = lists:foldl(fun(K, Acc) ->
                               case maps:get(K, Acc, undefined) of
                                   undefined -> Acc;
                                   PemBin ->
                                       CertFile = generate_filename(CertsDir, K),
                                       ok = save_cert_to_file(CertFile, PemBin),
                                       Acc#{K => CertFile}
                               end
                           end, SSLOpts, [<<"certfile">>, <<"keyfile">>, <<"cacertfile">>]),
            Config#{<<"ssl">> => NSSLOPts}
    end.

convert_certs(CertsDir, NewConfig, OldConfig) ->
    case maps:get(<<"ssl">>, NewConfig, undefined) of
        undefined ->
            NewConfig;
        NewSSLOpts ->
            OldSSLOpts = maps:get(<<"ssl">>, OldConfig, #{}),
            Diff = diff_certs(NewSSLOpts, OldSSLOpts),
            NSSLOpts = lists:foldl(fun({identical, K}, Acc) ->
                                    Acc#{K => maps:get(K, OldSSLOpts)};
                                    ({_, K}, Acc) ->
                                    CertFile = generate_filename(CertsDir, K),
                                    ok = save_cert_to_file(CertFile, maps:get(K, NewSSLOpts)),
                                    Acc#{K => CertFile}
                                end, NewSSLOpts, Diff),
            NewConfig#{<<"ssl">> => NSSLOpts}
    end.

clear_certs(CertsDir, Config) ->
    case maps:get(<<"ssl">>, Config, undefined) of
        undefined ->
            ok;
        SSLOpts ->
            lists:foreach(
                fun({_, Filename}) ->
                    _ = file:delete(filename:join([CertsDir, Filename]))
                end,
                maps:to_list(maps:with([<<"certfile">>, <<"keyfile">>, <<"cacertfile">>], SSLOpts)))
    end.

save_cert_to_file(Filename, PemBin) ->
    case public_key:pem_decode(PemBin) =/= [] of
        true ->
            case filelib:ensure_dir(Filename) of
                ok ->
                    case file:write_file(Filename, PemBin) of
                        ok -> ok;
                        {error, Reason} -> error({save_cert_to_file, {write_file, Reason}})
                    end;
                {error, Reason} ->
                    error({save_cert_to_file, {ensure_dir, Reason}})
            end;
        false ->
            error({save_cert_to_file, invalid_certificate})
    end.

generate_filename(CertsDir, Key) ->
    Prefix = case Key of
                 <<"keyfile">> -> "key-";
                 <<"certfile">> -> "cert-";
                 <<"cacertfile">> -> "cacert-"
             end,
    to_bin(filename:join([CertsDir, Prefix ++ emqx_misc:gen_id() ++ ".pem"])).

diff_certs(NewSSLOpts, OldSSLOpts) ->
    Keys = [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>],
    CertPems = maps:with(Keys, NewSSLOpts),
    CertFiles = maps:with(Keys, OldSSLOpts),
    Diff = lists:foldl(fun({K, CertFile}, Acc) ->
                    case maps:find(K, CertPems) of
                        error -> Acc;
                        {ok, PemBin1} ->
                            {ok, PemBin2} = file:read_file(CertFile),
                            case diff_cert(PemBin1, PemBin2) of
                                true ->
                                    [{changed, K} | Acc];
                                false ->
                                    [{identical, K} | Acc]
                            end
                    end
                end,
                [], maps:to_list(CertFiles)),
    Added = [{added, K} || K <- maps:keys(maps:without(maps:keys(CertFiles), CertPems))],
    Diff ++ Added.

diff_cert(Pem1, Pem2) ->
    cal_md5_for_cert(Pem1) =/= cal_md5_for_cert(Pem2).

cal_md5_for_cert(Pem) ->
    crypto:hash(md5, term_to_binary(public_key:pem_decode(Pem))).

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
    'mqtt:global';
global_chain('mqtt-sn') ->
    'mqtt-sn:global';
global_chain(coap) ->
    'coap:global';
global_chain(lwm2m) ->
    'lwm2m:global';
global_chain(stomp) ->
    'stomp:global';
global_chain(_) ->
    'unknown:global'.

maybe_hook(#{hooked := false} = State) ->
    case lists:any(fun(#chain{authenticators = []}) -> false;
                      (_) -> true
                   end, ets:tab2list(?CHAINS_TAB)) of
        true ->
            _ = emqx:hook('client.authenticate', {?MODULE, authenticate, []}),
            State#{hooked => true};
        false ->
            State
    end;
maybe_hook(State) ->
    State.

maybe_unhook(#{hooked := true} = State) ->
    case lists:all(fun(#chain{authenticators = []}) -> true;
                      (_) -> false
                   end, ets:tab2list(?CHAINS_TAB)) of
        true ->
            _ = emqx:unhook('client.authenticate', {?MODULE, authenticate, []}),
            State#{hooked => false};
        false ->
            State
    end;
maybe_unhook(State) ->
    State.

do_create_authenticator(ChainName, AuthenticatorID, #{enable := Enable} = Config, Providers) ->
    case maps:get(authn_type(Config), Providers, undefined) of
        undefined ->
            {error, no_available_provider};
        Provider ->
            Unique = unique(ChainName, AuthenticatorID, ?VER_1),
            case Provider:create(Config#{'_unique' => Unique}) of
                {ok, State} ->
                    Authenticator = #authenticator{id = AuthenticatorID,
                                                   provider = Provider,
                                                   enable = Enable,
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
                            {error, unsupported_operation}
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
                                       enable = Enable,
                                       state = State}) ->
    #{ id => ID
     , provider => Provider
     , enable => Enable
     , state => State
     }.

unique(ChainName, AuthenticatorID, Version) ->
    NChainName = atom_to_binary(ChainName),
    <<NChainName/binary, "/", AuthenticatorID/binary, ":", Version/binary>>.

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

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A).

call(Call) -> gen_server:call(?MODULE, Call, infinity).
