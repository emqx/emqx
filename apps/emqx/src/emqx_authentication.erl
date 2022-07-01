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

%% @doc Authenticator management API module.
%% Authentication is a core functionality of MQTT,
%% the 'emqx' APP provides APIs for other APPs to implement
%% the authentication callbacks.
-module(emqx_authentication).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_authentication.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(CONF_ROOT, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM).

%% The authentication entrypoint.
-export([authenticate/2]).

%% Authenticator manager process start/stop
-export([
    start_link/0,
    stop/0,
    get_providers/0
]).

%% Authenticator management APIs
-export([
    initialize_authentication/2,
    register_provider/2,
    register_providers/1,
    deregister_provider/1,
    deregister_providers/1,
    delete_chain/1,
    lookup_chain/1,
    list_chains/0,
    list_chain_names/0,
    create_authenticator/2,
    delete_authenticator/2,
    update_authenticator/3,
    lookup_authenticator/2,
    list_authenticators/1,
    move_authenticator/3
]).

%% APIs for observer built_in_database
-export([
    import_users/3,
    add_user/3,
    delete_user/3,
    update_user/4,
    lookup_user/3,
    list_users/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% utility functions
-export([authenticator_id/1, metrics_id/2]).

%% proxy callback
-export([
    pre_config_update/3,
    post_config_update/5
]).

-export_type([
    authenticator_id/0,
    position/0,
    chain_name/0
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(CHAINS_TAB, emqx_authn_chains).

-type chain_name() :: atom().
-type authenticator_id() :: binary().
-type position() :: front | rear | {before, authenticator_id()} | {'after', authenticator_id()}.
-type authn_type() :: atom() | {atom(), atom()}.
-type provider() :: module().

-type chain() :: #{
    name := chain_name(),
    authenticators := [authenticator()]
}.

-type authenticator() :: #{
    id := authenticator_id(),
    provider := provider(),
    enable := boolean(),
    state := map()
}.

-type config() :: emqx_authentication_config:config().
-type state() :: #{atom() => term()}.
-type extra() :: #{
    is_superuser := boolean(),
    atom() => term()
}.
-type user_info() :: #{
    user_id := binary(),
    atom() => term()
}.

%% @doc check_config takes raw config from config file,
%% parse and validate it, and return parsed result.
-callback check_config(config()) -> config().

-callback create(AuthenticatorID, Config) ->
    {ok, State}
    | {error, term()}
when
    AuthenticatorID :: authenticator_id(), Config :: config(), State :: state().

-callback update(Config, State) ->
    {ok, NewState}
    | {error, term()}
when
    Config :: config(), State :: state(), NewState :: state().

-callback authenticate(Credential, State) ->
    ignore
    | {ok, Extra}
    | {ok, Extra, AuthData}
    | {continue, AuthCache}
    | {continue, AuthData, AuthCache}
    | {error, term()}
when
    Credential :: map(),
    State :: state(),
    Extra :: extra(),
    AuthData :: binary(),
    AuthCache :: map().

-callback destroy(State) ->
    ok
when
    State :: state().

-callback import_users({Filename, FileData}, State) ->
    ok
    | {error, term()}
when
    Filename :: binary(), FileData :: binary(), State :: state().

-callback add_user(UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserInfo :: user_info(), State :: state(), User :: user_info().

-callback delete_user(UserID, State) ->
    ok
    | {error, term()}
when
    UserID :: binary(), State :: state().

-callback update_user(UserID, UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserID :: binary(), UserInfo :: map(), State :: state(), User :: user_info().

-callback lookup_user(UserID, UserInfo, State) ->
    {ok, User}
    | {error, term()}
when
    UserID :: binary(), UserInfo :: map(), State :: state(), User :: user_info().

-callback list_users(State) ->
    {ok, Users}
when
    State :: state(), Users :: [user_info()].

-optional_callbacks([
    import_users/2,
    add_user/2,
    delete_user/2,
    update_user/3,
    lookup_user/3,
    list_users/1,
    check_config/1
]).

%%------------------------------------------------------------------------------
%% Authenticate
%%------------------------------------------------------------------------------

authenticate(#{enable_authn := false}, _AuthResult) ->
    inc_authenticate_metric('authentication.success.anonymous'),
    ignore;
authenticate(#{listener := Listener, protocol := Protocol} = Credential, _AuthResult) ->
    case get_authenticators(Listener, global_chain(Protocol)) of
        {ok, ChainName, Authenticators} ->
            case get_enabled(Authenticators) of
                [] ->
                    inc_authenticate_metric('authentication.success.anonymous'),
                    ignore;
                NAuthenticators ->
                    Result = do_authenticate(ChainName, NAuthenticators, Credential),

                    case Result of
                        {stop, {ok, _}} ->
                            inc_authenticate_metric('authentication.success');
                        {stop, {error, _}} ->
                            inc_authenticate_metric('authentication.failure');
                        _ ->
                            ok
                    end,
                    Result
            end;
        none ->
            inc_authenticate_metric('authentication.success.anonymous'),
            ignore
    end.

get_authenticators(Listener, Global) ->
    case ets:lookup(?CHAINS_TAB, Listener) of
        [#chain{name = Name, authenticators = Authenticators}] ->
            {ok, Name, Authenticators};
        _ ->
            case ets:lookup(?CHAINS_TAB, Global) of
                [#chain{name = Name, authenticators = Authenticators}] ->
                    {ok, Name, Authenticators};
                _ ->
                    none
            end
    end.

get_enabled(Authenticators) ->
    [Authenticator || Authenticator <- Authenticators, Authenticator#authenticator.enable =:= true].

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

pre_config_update(Path, UpdateReq, OldConfig) ->
    emqx_authentication_config:pre_config_update(Path, UpdateReq, OldConfig).

post_config_update(Path, UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    emqx_authentication_config:post_config_update(Path, UpdateReq, NewConfig, OldConfig, AppEnvs).

%% @doc Get all registered authentication providers.
get_providers() ->
    call(get_providers).

%% @doc Get authenticator identifier from its config.
%% The authenticator config must contain a 'mechanism' key
%% and maybe a 'backend' key.
%% This function works with both parsed (atom keys) and raw (binary keys)
%% configurations.
authenticator_id(Config) ->
    emqx_authentication_config:authenticator_id(Config).

%% @doc Call this API to initialize authenticators implemented in another APP.
-spec initialize_authentication(chain_name(), [config()]) -> ok.
initialize_authentication(_, []) ->
    ok;
initialize_authentication(ChainName, AuthenticatorsConfig) ->
    CheckedConfig = to_list(AuthenticatorsConfig),
    lists:foreach(
        fun(AuthenticatorConfig) ->
            case create_authenticator(ChainName, AuthenticatorConfig) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "failed_to_create_authenticator",
                        authenticator => authenticator_id(AuthenticatorConfig),
                        reason => Reason
                    })
            end
        end,
        CheckedConfig
    ).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    %% Create chains ETS table here so that it belongs to the supervisor
    %% and survives `emqx_authentication` crashes.
    ok = create_chain_table(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Register authentication providers.
%% A provider is a tuple of `AuthNType' the module which implements
%% the authenticator callbacks.
%% For example, ``[{{'password_based', redis}, emqx_authn_redis}]''
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

-spec delete_chain(chain_name()) -> ok | {error, term()}.
delete_chain(Name) ->
    call({delete_chain, Name}).

-spec lookup_chain(chain_name()) -> {ok, chain()} | {error, term()}.
lookup_chain(Name) ->
    case ets:lookup(?CHAINS_TAB, Name) of
        [] ->
            {error, {not_found, {chain, Name}}};
        [Chain] ->
            {ok, serialize_chain(Chain)}
    end.

-spec list_chains() -> {ok, [chain()]}.
list_chains() ->
    Chains = ets:tab2list(?CHAINS_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

-spec list_chain_names() -> {ok, [atom()]}.
list_chain_names() ->
    Select = ets:fun2ms(fun(#chain{name = Name}) -> Name end),
    ChainNames = ets:select(?CHAINS_TAB, Select),
    {ok, ChainNames}.

-spec create_authenticator(chain_name(), config()) -> {ok, authenticator()} | {error, term()}.
create_authenticator(ChainName, Config) ->
    call({create_authenticator, ChainName, Config}).

-spec delete_authenticator(chain_name(), authenticator_id()) -> ok | {error, term()}.
delete_authenticator(ChainName, AuthenticatorID) ->
    call({delete_authenticator, ChainName, AuthenticatorID}).

-spec update_authenticator(chain_name(), authenticator_id(), config()) ->
    {ok, authenticator()} | {error, term()}.
update_authenticator(ChainName, AuthenticatorID, Config) ->
    call({update_authenticator, ChainName, AuthenticatorID, Config}).

-spec lookup_authenticator(chain_name(), authenticator_id()) ->
    {ok, authenticator()} | {error, term()}.
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

-spec import_users(chain_name(), authenticator_id(), {binary(), binary()}) ->
    ok | {error, term()}.
import_users(ChainName, AuthenticatorID, Filename) ->
    call({import_users, ChainName, AuthenticatorID, Filename}).

-spec add_user(chain_name(), authenticator_id(), user_info()) ->
    {ok, user_info()} | {error, term()}.
add_user(ChainName, AuthenticatorID, UserInfo) ->
    call({add_user, ChainName, AuthenticatorID, UserInfo}).

-spec delete_user(chain_name(), authenticator_id(), binary()) -> ok | {error, term()}.
delete_user(ChainName, AuthenticatorID, UserID) ->
    call({delete_user, ChainName, AuthenticatorID, UserID}).

-spec update_user(chain_name(), authenticator_id(), binary(), map()) ->
    {ok, user_info()} | {error, term()}.
update_user(ChainName, AuthenticatorID, UserID, NewUserInfo) ->
    call({update_user, ChainName, AuthenticatorID, UserID, NewUserInfo}).

-spec lookup_user(chain_name(), authenticator_id(), binary()) ->
    {ok, user_info()} | {error, term()}.
lookup_user(ChainName, AuthenticatorID, UserID) ->
    call({lookup_user, ChainName, AuthenticatorID, UserID}).

-spec list_users(chain_name(), authenticator_id(), map()) -> {ok, [user_info()]} | {error, term()}.
list_users(ChainName, AuthenticatorID, FuzzyParams) ->
    call({list_users, ChainName, AuthenticatorID, FuzzyParams}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    process_flag(trap_exit, true),
    ok = emqx_config_handler:add_handler([?CONF_ROOT], ?MODULE),
    ok = emqx_config_handler:add_handler([listeners, '?', '?', ?CONF_ROOT], ?MODULE),
    {ok, #{hooked => false, providers => #{}}}.

handle_call(get_providers, _From, #{providers := Providers} = State) ->
    reply(Providers, State);
handle_call(
    {register_providers, Providers},
    _From,
    #{providers := Reg0} = State
) ->
    case lists:filter(fun({T, _}) -> maps:is_key(T, Reg0) end, Providers) of
        [] ->
            Reg = lists:foldl(
                fun({AuthNType, Module}, Pin) ->
                    Pin#{AuthNType => Module}
                end,
                Reg0,
                Providers
            ),
            reply(ok, State#{providers := Reg});
        Clashes ->
            reply({error, {authentication_type_clash, Clashes}}, State)
    end;
handle_call({deregister_providers, AuthNTypes}, _From, #{providers := Providers} = State) ->
    reply(ok, State#{providers := maps:without(AuthNTypes, Providers)});
handle_call({delete_chain, ChainName}, _From, State) ->
    UpdateFun = fun(Chain) ->
        {_MatchedIDs, NewChain} = do_delete_authenticators(fun(_) -> true end, Chain),
        {ok, ok, NewChain}
    end,
    Reply = with_chain(ChainName, UpdateFun),
    reply(Reply, maybe_unhook(State));
handle_call({create_authenticator, ChainName, Config}, _From, #{providers := Providers} = State) ->
    UpdateFun = fun(Chain) ->
        handle_create_authenticator(Chain, Config, Providers)
    end,
    Reply = with_new_chain(ChainName, UpdateFun),
    reply(Reply, maybe_hook(State));
handle_call({delete_authenticator, ChainName, AuthenticatorID}, _From, State) ->
    UpdateFun = fun(Chain) ->
        handle_delete_authenticator(Chain, AuthenticatorID)
    end,
    Reply = with_chain(ChainName, UpdateFun),
    reply(Reply, maybe_unhook(State));
handle_call({update_authenticator, ChainName, AuthenticatorID, Config}, _From, State) ->
    UpdateFun = fun(Chain) ->
        handle_update_authenticator(Chain, AuthenticatorID, Config)
    end,
    Reply = with_chain(ChainName, UpdateFun),
    reply(Reply, State);
handle_call({move_authenticator, ChainName, AuthenticatorID, Position}, _From, State) ->
    UpdateFun = fun(Chain) ->
        handle_move_authenticator(Chain, AuthenticatorID, Position)
    end,
    Reply = with_chain(ChainName, UpdateFun),
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
handle_call({list_users, ChainName, AuthenticatorID, FuzzyParams}, _From, State) ->
    Reply = call_authenticator(ChainName, AuthenticatorID, list_users, [FuzzyParams]),
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

terminate(Reason, _State) ->
    case Reason of
        {shutdown, _} ->
            ok;
        Reason when Reason == normal; Reason == shutdown ->
            ok;
        Other ->
            ?SLOG(error, #{
                msg => "emqx_authentication_terminating",
                reason => Other
            })
    end,
    emqx_config_handler:remove_handler([?CONF_ROOT]),
    emqx_config_handler:remove_handler([listeners, '?', '?', ?CONF_ROOT]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

handle_update_authenticator(Chain, AuthenticatorID, Config) ->
    #chain{authenticators = Authenticators} = Chain,
    case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
        false ->
            {error, {not_found, {authenticator, AuthenticatorID}}};
        #authenticator{provider = Provider, state = ST} = Authenticator ->
            case AuthenticatorID =:= authenticator_id(Config) of
                true ->
                    NConfig = insert_user_group(Chain, Config),
                    case Provider:update(NConfig, ST) of
                        {ok, NewST} ->
                            NewAuthenticator = Authenticator#authenticator{
                                state = NewST,
                                enable = maps:get(enable, NConfig)
                            },
                            NewAuthenticators = replace_authenticator(
                                AuthenticatorID,
                                NewAuthenticator,
                                Authenticators
                            ),
                            NewChain = Chain#chain{authenticators = NewAuthenticators},
                            Result = {ok, serialize_authenticator(NewAuthenticator)},
                            {ok, Result, NewChain};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                false ->
                    {error, change_of_authentication_type_is_not_allowed}
            end
    end.

handle_delete_authenticator(Chain, AuthenticatorID) ->
    MatchFun = fun(#authenticator{id = ID}) ->
        ID =:= AuthenticatorID
    end,
    case do_delete_authenticators(MatchFun, Chain) of
        {[], _NewChain} ->
            {error, {not_found, {authenticator, AuthenticatorID}}};
        {[AuthenticatorID], NewChain} ->
            {ok, ok, NewChain}
    end.

handle_move_authenticator(Chain, AuthenticatorID, Position) ->
    #chain{authenticators = Authenticators} = Chain,
    case do_move_authenticator(AuthenticatorID, Authenticators, Position) of
        {ok, NAuthenticators} ->
            NewChain = Chain#chain{authenticators = NAuthenticators},
            {ok, ok, NewChain};
        {error, Reason} ->
            {error, Reason}
    end.

handle_create_authenticator(Chain, Config, Providers) ->
    #chain{name = Name, authenticators = Authenticators} = Chain,
    AuthenticatorID = authenticator_id(Config),
    case lists:keymember(AuthenticatorID, #authenticator.id, Authenticators) of
        true ->
            {error, {already_exists, {authenticator, AuthenticatorID}}};
        false ->
            NConfig = insert_user_group(Chain, Config),
            case do_create_authenticator(AuthenticatorID, NConfig, Providers) of
                {ok, Authenticator} ->
                    NAuthenticators =
                        Authenticators ++
                            [Authenticator#authenticator{enable = maps:get(enable, Config)}],
                    ok = emqx_metrics_worker:create_metrics(
                        authn_metrics,
                        metrics_id(Name, AuthenticatorID),
                        [total, success, failed, nomatch],
                        [total]
                    ),
                    NewChain = Chain#chain{authenticators = NAuthenticators},
                    Result = {ok, serialize_authenticator(Authenticator)},
                    {ok, Result, NewChain};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

do_authenticate(_ChainName, [], _) ->
    {stop, {error, not_authorized}};
do_authenticate(
    ChainName, [#authenticator{id = ID, provider = Provider, state = State} | More], Credential
) ->
    MetricsID = metrics_id(ChainName, ID),
    emqx_metrics_worker:inc(authn_metrics, MetricsID, total),
    try Provider:authenticate(Credential, State) of
        ignore ->
            ok = emqx_metrics_worker:inc(authn_metrics, MetricsID, nomatch),
            do_authenticate(ChainName, More, Credential);
        Result ->
            %% {ok, Extra}
            %% {ok, Extra, AuthData}
            %% {continue, AuthCache}
            %% {continue, AuthData, AuthCache}
            %% {error, Reason}
            case Result of
                {ok, _} ->
                    emqx_metrics_worker:inc(authn_metrics, MetricsID, success);
                {error, _} ->
                    emqx_metrics_worker:inc(authn_metrics, MetricsID, failed);
                _ ->
                    ok
            end,
            {stop, Result}
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "unexpected_error_in_authentication",
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace,
                authenticator => ID
            }),
            emqx_metrics_worker:inc(authn_metrics, MetricsID, nomatch),
            do_authenticate(ChainName, More, Credential)
    end.

reply(Reply, State) ->
    {reply, Reply, State}.

save_chain(#chain{
    name = Name,
    authenticators = []
}) ->
    ets:delete(?CHAINS_TAB, Name);
save_chain(#chain{} = Chain) ->
    ets:insert(?CHAINS_TAB, Chain).

create_chain_table() ->
    try
        _ = ets:new(?CHAINS_TAB, [
            named_table,
            set,
            public,
            {keypos, #chain.name},
            {read_concurrency, true}
        ]),
        ok
    catch
        error:badarg -> ok
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
    case
        lists:any(
            fun
                (#chain{authenticators = []}) -> false;
                (_) -> true
            end,
            ets:tab2list(?CHAINS_TAB)
        )
    of
        true ->
            ok = emqx_hooks:put('client.authenticate', {?MODULE, authenticate, []}, ?HP_AUTHN),
            State#{hooked => true};
        false ->
            State
    end;
maybe_hook(State) ->
    State.

maybe_unhook(#{hooked := true} = State) ->
    case
        lists:all(
            fun
                (#chain{authenticators = []}) -> true;
                (_) -> false
            end,
            ets:tab2list(?CHAINS_TAB)
        )
    of
        true ->
            ok = emqx_hooks:del('client.authenticate', {?MODULE, authenticate, []}),
            State#{hooked => false};
        false ->
            State
    end;
maybe_unhook(State) ->
    State.

do_create_authenticator(AuthenticatorID, #{enable := Enable} = Config, Providers) ->
    case maps:get(authn_type(Config), Providers, undefined) of
        undefined ->
            {error, no_available_provider};
        Provider ->
            case Provider:create(AuthenticatorID, Config) of
                {ok, State} ->
                    Authenticator = #authenticator{
                        id = AuthenticatorID,
                        provider = Provider,
                        enable = Enable,
                        state = State
                    },
                    {ok, Authenticator};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

do_delete_authenticators(MatchFun, #chain{name = Name, authenticators = Authenticators} = Chain) ->
    {Matching, Others} = lists:partition(MatchFun, Authenticators),

    MatchingIDs = lists:map(
        fun(#authenticator{id = ID}) -> ID end,
        Matching
    ),

    ok = lists:foreach(
        fun(#authenticator{id = ID} = Authenticator) ->
            do_destroy_authenticator(Authenticator),
            emqx_metrics_worker:clear_metrics(authn_metrics, metrics_id(Name, ID))
        end,
        Matching
    ),
    {MatchingIDs, Chain#chain{authenticators = Others}}.

do_destroy_authenticator(#authenticator{provider = Provider, state = State}) ->
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
                ?CMD_MOVE_FRONT ->
                    {ok, [Authenticator | NAuthenticators]};
                ?CMD_MOVE_REAR ->
                    {ok, NAuthenticators ++ [Authenticator]};
                ?CMD_MOVE_BEFORE(RelatedID) ->
                    insert(Authenticator, NAuthenticators, ?CMD_MOVE_BEFORE(RelatedID), []);
                ?CMD_MOVE_AFTER(RelatedID) ->
                    insert(Authenticator, NAuthenticators, ?CMD_MOVE_AFTER(RelatedID), [])
            end
    end.

insert(_, [], {_, RelatedID}, _) ->
    {error, {not_found, {authenticator, RelatedID}}};
insert(
    Authenticator,
    [#authenticator{id = RelatedID} = Related | Rest],
    {Relative, RelatedID},
    Acc
) ->
    case Relative of
        before ->
            {ok, lists:reverse(Acc) ++ [Authenticator, Related | Rest]};
        'after' ->
            {ok, lists:reverse(Acc) ++ [Related, Authenticator | Rest]}
    end;
insert(Authenticator, [Authenticator0 | More], {Relative, RelatedID}, Acc) ->
    insert(Authenticator, More, {Relative, RelatedID}, [Authenticator0 | Acc]).

with_new_chain(ChainName, Fun) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            Chain = #chain{name = ChainName, authenticators = []},
            do_with_chain(Fun, Chain);
        [Chain] ->
            do_with_chain(Fun, Chain)
    end.

with_chain(ChainName, Fun) ->
    case ets:lookup(?CHAINS_TAB, ChainName) of
        [] ->
            {error, {not_found, {chain, ChainName}}};
        [Chain] ->
            do_with_chain(Fun, Chain)
    end.

do_with_chain(Fun, Chain) ->
    try
        case Fun(Chain) of
            {ok, Result} ->
                Result;
            {ok, Result, NewChain} ->
                save_chain(NewChain),
                Result;
            {error, _} = Error ->
                Error
        end
    catch
        Class:Reason:Stk ->
            {error, {exception, {Class, Reason, Stk}}}
    end.

call_authenticator(ChainName, AuthenticatorID, Func, Args) ->
    Fun =
        fun(#chain{authenticators = Authenticators}) ->
            case lists:keyfind(AuthenticatorID, #authenticator.id, Authenticators) of
                false ->
                    {error, {not_found, {authenticator, AuthenticatorID}}};
                #authenticator{provider = Provider, state = State} ->
                    case erlang:function_exported(Provider, Func, length(Args) + 1) of
                        true ->
                            {ok, erlang:apply(Provider, Func, Args ++ [State])};
                        false ->
                            {error, unsupported_operation}
                    end
            end
        end,
    with_chain(ChainName, Fun).

serialize_chain(#chain{
    name = Name,
    authenticators = Authenticators
}) ->
    #{
        name => Name,
        authenticators => serialize_authenticators(Authenticators)
    }.

serialize_authenticators(Authenticators) ->
    [serialize_authenticator(Authenticator) || Authenticator <- Authenticators].

serialize_authenticator(#authenticator{
    id = ID,
    provider = Provider,
    enable = Enable,
    state = State
}) ->
    #{
        id => ID,
        provider => Provider,
        enable => Enable,
        state => State
    }.

authn_type(#{mechanism := Mechanism, backend := Backend}) ->
    {Mechanism, Backend};
authn_type(#{mechanism := Mechanism}) ->
    Mechanism.

insert_user_group(
    Chain,
    Config = #{
        mechanism := password_based,
        backend := built_in_database
    }
) ->
    Config#{user_group => Chain#chain.name};
insert_user_group(_Chain, Config) ->
    Config.

metrics_id(ChainName, AuthenticatorId) ->
    iolist_to_binary([atom_to_binary(ChainName), <<"-">>, AuthenticatorId]).

to_list(undefined) -> [];
to_list(M) when M =:= #{} -> [];
to_list(M) when is_map(M) -> [M];
to_list(L) when is_list(L) -> L.

call(Call) -> gen_server:call(?MODULE, Call, infinity).

inc_authenticate_metric('authentication.success.anonymous' = Metric) ->
    emqx_metrics:inc(Metric),
    emqx_metrics:inc('authentication.success');
inc_authenticate_metric(Metric) ->
    emqx_metrics:inc(Metric).
