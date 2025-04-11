%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_config).

-behaviour(emqx_config_handler).

-export([load/0, unload/0]).

-export([pre_config_update/3, post_config_update/5]).

-export([
    get_provider/1,
    get_completion_profile/1
]).

-export([
    update_providers_raw/1,
    get_providers_raw/0,
    update_completion_profiles_raw/1,
    get_completion_profiles_raw/0
]).

%%--------------------------------------------------------------------
%% Definitions
%%--------------------------------------------------------------------

-define(CREDENTIAL_PT_KEY, {?MODULE, ai_providers}).
-define(COMPLETION_PROFILE_PT_KEY, {?MODULE, ai_completion_profiles}).

-define(CREDENTIAL_CONFIG_PATH, [ai, providers]).
-define(COMPLETION_PROFILE_CONFIG_PATH, [ai, completion_profiles]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type raw_provider() :: map().
-type raw_completion_profile() :: map().
-type provider_name() :: binary().
-type completion_profile_name() :: binary().
-type transport_options() :: #{
    connect_timeout => pos_integer(),
    recv_timeout => pos_integer(),
    checkout_timeout => pos_integer()
}.

-type ai_type() :: openai | anthropic.

-type provider() :: #{
    name := provider_name(),
    type := ai_type(),
    api_key := fun(() -> binary()),
    transport_options := transport_options()
}.

-type completion_profile() :: #{
    name := completion_profile_name(),
    type := ai_type(),
    provider := provider(),
    _ => _
}.

-type update_providers_request() ::
    {add, raw_provider()}
    | {update, raw_provider()}
    | {delete, provider_name()}.

-type update_completion_profiles_request() ::
    {add, raw_completion_profile()}
    | {update, raw_completion_profile()}
    | {delete, completion_profile_name()}.

-type update_providers_error_reason() ::
    duplicate_provider_name
    | provider_in_use
    | completion_profile_provider_type_mismatch
    | provider_not_found.

-type update_completion_profiles_error_reason() ::
    duplicate_completion_profile_name
    | completion_profile_provider_not_found
    | completion_profile_provider_type_mismatch
    | completion_profile_not_found.

-type update_providers_error() ::
    #{reason => update_providers_error_reason(), _ => _}.
-type update_completion_profiles_error() ::
    #{reason => update_completion_profiles_error_reason(), _ => _}.

-export_type([
    ai_type/0,
    provider/0,
    completion_profile/0,
    transport_options/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Load and unload

-spec load() -> ok.
load() ->
    ok = emqx_config_handler:add_handler(?CREDENTIAL_CONFIG_PATH, ?MODULE),
    ok = emqx_config_handler:add_handler(?COMPLETION_PROFILE_CONFIG_PATH, ?MODULE),
    ok = cache_providers(),
    ok = cache_completion_profiles().

-spec unload() -> ok.
unload() ->
    _ = persistent_term:erase(?CREDENTIAL_PT_KEY),
    _ = persistent_term:erase(?COMPLETION_PROFILE_PT_KEY),
    ok = emqx_config_handler:remove_handler(?CREDENTIAL_CONFIG_PATH),
    ok = emqx_config_handler:remove_handler(?COMPLETION_PROFILE_CONFIG_PATH).

%% Config handler callbacks

%%
%% Provider update
%%
pre_config_update(
    ?CREDENTIAL_CONFIG_PATH, {add, #{<<"name">> := Name} = Provider}, OldProviders
) ->
    case find_provider(Name, OldProviders) of
        {error, #{reason := provider_not_found}} ->
            {ok, OldProviders ++ [Provider]};
        {ok, _} ->
            {error, #{
                reason => duplicate_provider_name,
                provider => Name
            }}
    end;
pre_config_update(
    ?CREDENTIAL_CONFIG_PATH, {update, #{<<"name">> := Name} = Provider}, OldProviders
) ->
    maybe
        {ok, OldProvider, NewProviders} ?= update_provider(Name, Provider, OldProviders),
        ok ?= validate_provider_update(OldProvider, Provider),
        {ok, NewProviders}
    end;
pre_config_update(?CREDENTIAL_CONFIG_PATH, {delete, Name}, OldProviders) ->
    maybe
        {ok, OldProvider, NewProviders} ?= remove_provider(Name, OldProviders),
        ok ?= validate_provider_not_used(OldProvider),
        {ok, NewProviders}
    end;
pre_config_update(?CREDENTIAL_CONFIG_PATH, NewProviders, _OldProviders) when
    is_list(NewProviders)
->
    validate_provider_presence(NewProviders, get_completion_profiles_raw());
%%
%% Completion profile update
%%
pre_config_update(
    ?COMPLETION_PROFILE_CONFIG_PATH, {add, #{<<"name">> := Name} = Profile}, OldProfiles
) ->
    case find_completion_profile(Name, OldProfiles) of
        {error, #{reason := completion_profile_not_found}} ->
            maybe
                ok ?= validate_completion_profile_add(Profile),
                {ok, OldProfiles ++ [Profile]}
            end;
        {ok, _} ->
            {error, #{
                reason => duplicate_completion_profile_name,
                profile_name => Name
            }}
    end;
pre_config_update(
    ?COMPLETION_PROFILE_CONFIG_PATH, {update, #{<<"name">> := Name} = Profile}, OldProfiles
) ->
    maybe
        {ok, _OldProfile, NewProfiles} ?= update_completion_profile(Name, Profile, OldProfiles),
        ok ?= validate_completion_profile_update(Profile),
        {ok, NewProfiles}
    end;
pre_config_update(?COMPLETION_PROFILE_CONFIG_PATH, {delete, Name}, OldProfiles) ->
    maybe
        {ok, _OldProfile, NewProfiles} ?= remove_completion_profile(Name, OldProfiles),
        {ok, NewProfiles}
    end;
pre_config_update(?COMPLETION_PROFILE_CONFIG_PATH, NewProfiles, _OldProfiles) when
    is_list(NewProfiles)
->
    validate_provider_presence(get_providers_raw(), NewProfiles);
pre_config_update(_Path, Request, _OldConf) ->
    {error, #{
        reason => bad_config,
        config => Request
    }}.

post_config_update(?CREDENTIAL_CONFIG_PATH, _Request, NewConf, _OldConf, _AppEnvs) ->
    ok = cache_providers(NewConf);
post_config_update(?COMPLETION_PROFILE_CONFIG_PATH, _Request, NewConf, _OldConf, _AppEnvs) ->
    ok = cache_completion_profiles(NewConf).

%% Config accessors

-spec update_providers_raw(update_providers_request()) ->
    ok
    | {error, update_providers_error()}
    %% Unexpected emqx_conf errors
    | {error, term()}.
update_providers_raw(Request) ->
    wrap_config_update_error(
        emqx_conf:update(?CREDENTIAL_CONFIG_PATH, Request, #{override_to => cluster})
    ).

-spec get_providers_raw() -> [raw_provider()].
get_providers_raw() ->
    emqx_config:get_raw([ai, providers], []).

-spec update_completion_profiles_raw(update_completion_profiles_request()) ->
    ok
    | {error, update_completion_profiles_error()}
    %% Unexpected emqx_conf errors
    | {error, term()}.
update_completion_profiles_raw(Request) ->
    wrap_config_update_error(
        emqx_conf:update(?COMPLETION_PROFILE_CONFIG_PATH, Request, #{override_to => cluster})
    ).

-spec get_completion_profiles_raw() -> [raw_completion_profile()].
get_completion_profiles_raw() ->
    emqx_config:get_raw([ai, completion_profiles], []).

-spec get_provider(provider_name()) -> {ok, provider()} | not_found.
get_provider(Name) ->
    case persistent_term:get(?CREDENTIAL_PT_KEY, #{}) of
        #{Name := Provider} ->
            {ok, Provider};
        _ ->
            not_found
    end.

-spec get_completion_profile(completion_profile_name()) -> {ok, completion_profile()} | not_found.
get_completion_profile(Name) ->
    case persistent_term:get(?COMPLETION_PROFILE_PT_KEY, #{}) of
        #{Name := #{provider_name := ProviderName} = CompletionProfile0} ->
            maybe
                {ok, Provider} ?= get_provider(ProviderName),
                CompletionProfile1 = maps:without([provider_name], CompletionProfile0),
                CompletionProfile = CompletionProfile1#{provider => Provider},
                {ok, CompletionProfile}
            end;
        _ ->
            not_found
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Cache management

cache_providers() ->
    cache_providers(emqx_config:get(?CREDENTIAL_CONFIG_PATH, [])).

cache_providers(Config) ->
    Providers = maps:from_list(
        lists:map(
            fun(#{name := Name} = Provider) ->
                {Name, Provider}
            end,
            Config
        )
    ),
    persistent_term:put(?CREDENTIAL_PT_KEY, Providers).

cache_completion_profiles() ->
    cache_completion_profiles(emqx_config:get(?COMPLETION_PROFILE_CONFIG_PATH, [])).

cache_completion_profiles(Config) ->
    CompletionProfiles = maps:from_list(
        lists:map(
            fun(#{name := Name} = CompletionProfile) ->
                {Name, CompletionProfile}
            end,
            Config
        )
    ),
    persistent_term:put(?COMPLETION_PROFILE_PT_KEY, CompletionProfiles).

%% Provider validations

validate_provider_update(#{<<"type">> := Type} = _Old, #{<<"type">> := Type} = _New) ->
    ok;
validate_provider_update(OldProvider, _NewProvider) ->
    validate_provider_not_used(OldProvider).

validate_provider_not_used(#{<<"name">> := Name} = _Provider) ->
    UsingProfiles = lists:filter(
        fun(#{<<"provider_name">> := N}) -> N =:= Name end,
        get_completion_profiles_raw()
    ),
    case UsingProfiles of
        [] ->
            ok;
        _ ->
            {error, #{
                reason => provider_in_use,
                provider_name => Name
            }}
    end.

%% Completion profile validations

validate_provider_presence(Providers, CompletionProfiles) ->
    try
        lists:foreach(
            fun(CompletionProfile) ->
                case validate_provider_presence_for_profile(CompletionProfile, Providers) of
                    ok ->
                        ok;
                    {error, Error} ->
                        throw(Error)
                end
            end,
            CompletionProfiles
        )
    catch
        throw:Error ->
            {error, Error}
    end.

validate_provider_presence_for_profile(
    #{<<"provider_name">> := Name, <<"type">> := Type, <<"name">> := ProfileName}, Providers
) ->
    case find_provider(Name, Providers) of
        {ok, #{<<"type">> := Type}} ->
            ok;
        {ok, #{<<"type">> := OtherType}} ->
            {error, #{
                reason => completion_profile_provider_type_mismatch,
                profile_name => ProfileName,
                profile_type => Type,
                provider_name => Name,
                provider_type => OtherType
            }};
        {error, #{reason := provider_not_found}} ->
            {error, #{
                reason => completion_profile_provider_not_found,
                profile_name => ProfileName,
                provider_name => Name
            }}
    end.

validate_completion_profile_update(CompletionProfile) ->
    validate_provider_presence_for_profile(CompletionProfile, get_providers_raw()).

validate_completion_profile_add(CompletionProfile) ->
    validate_provider_presence_for_profile(CompletionProfile, get_providers_raw()).

%% Completion profile management

find_completion_profile(Name, Profiles) ->
    wrap_completion_profile_not_found(
        Name,
        find_by_key(<<"name">>, Name, Profiles)
    ).

remove_completion_profile(Name, Profiles) ->
    wrap_completion_profile_not_found(
        Name,
        remove_by_key(<<"name">>, Name, Profiles)
    ).

update_completion_profile(Name, Profile, Profiles) ->
    wrap_completion_profile_not_found(
        Name,
        update_by_key(<<"name">>, Name, Profile, Profiles)
    ).

wrap_completion_profile_not_found(Name, not_found) ->
    {error, #{
        reason => completion_profile_not_found,
        profile_name => Name
    }};
wrap_completion_profile_not_found(_Name, Result) ->
    Result.

%% Provider management

find_provider(Name, Providers) ->
    wrap_provider_not_found(
        Name,
        find_by_key(<<"name">>, Name, Providers)
    ).

remove_provider(Name, Providers) ->
    wrap_provider_not_found(
        Name,
        remove_by_key(<<"name">>, Name, Providers)
    ).

update_provider(Name, Provider, Providers) ->
    wrap_provider_not_found(
        Name,
        update_by_key(<<"name">>, Name, Provider, Providers)
    ).

%% Helpers

wrap_provider_not_found(Name, not_found) ->
    {error, #{
        reason => provider_not_found,
        provider_name => Name
    }};
wrap_provider_not_found(_Name, Result) ->
    Result.

wrap_config_update_error({error, {pre_config_update, ?MODULE, Error}}) ->
    {error, Error};
wrap_config_update_error({error, {post_config_update, ?MODULE, Error}}) ->
    {error, Error};
wrap_config_update_error({error, Error}) ->
    {error, Error};
wrap_config_update_error({ok, #{config := _Config}}) ->
    ok.

find_by_key(Key, Value, Entries) ->
    MatchingEntries = lists:filter(fun(#{Key := V} = _Entry) -> V =:= Value end, Entries),
    case MatchingEntries of
        [Entry] ->
            {ok, Entry};
        [] ->
            not_found;
        _ ->
            error({multiple_entries_with_value, Key, Value})
    end.

remove_by_key(Key, Value, Entries) ->
    case lists:splitwith(fun(#{Key := V} = _Entry) -> V =/= Value end, Entries) of
        {_, []} ->
            not_found;
        {Left, [OldEntry | Right]} ->
            {ok, OldEntry, Left ++ Right}
    end.

update_by_key(Key, Value, Entry, Entries) ->
    case lists:splitwith(fun(#{Key := V} = _Entry) -> V =/= Value end, Entries) of
        {_, []} ->
            not_found;
        {Left, [OldEntry | Right]} ->
            {ok, OldEntry, Left ++ [Entry | Right]}
    end.
