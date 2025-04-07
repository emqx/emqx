%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_config).

-include_lib("hocon/include/hocon.hrl").

-behaviour(emqx_config_handler).

-export([load/0, unload/0]).

-export([pre_config_update/3, post_config_update/5]).

-export([get_credential/1, get_completion_profile/1]).

%%--------------------------------------------------------------------
%% Definitions
%%--------------------------------------------------------------------

-define(CREDENTIAL_PT_KEY, {?MODULE, ai_credentials}).
-define(COMPLETION_PROFILE_PT_KEY, {?MODULE, ai_completion_profiles}).

-define(CREDENTIAL_CONFIG_PATH, [ai, credentials]).
-define(COMPLETION_PROFILE_CONFIG_PATH, [ai, completion_profiles]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type config_path() :: list(atom()).
-type raw_credential() :: map().
-type raw_completion_profile() :: map().
-type credential_name() :: binary().
-type completion_profile_name() :: binary().

-type ai_type() :: openai | anthropic.

-type credential() :: #{
    name := credential_name(),
    type := ai_type(),
    api_key := fun(() -> binary())
}.

-type completion_profile() :: #{
    name := completion_profile_name(),
    type := ai_type(),
    credential := credential(),
    system_prompt := binary(),
    model := binary()
}.

-type update_credentials_request() ::
    {add, raw_credential()}
    | {update, raw_credential()}
    | {delete, credential_name()}.

-type update_completion_profiles_request() ::
    {add, raw_completion_profile()}
    | {update, raw_completion_profile()}
    | {delete, completion_profile_name()}.

-type update_credentials_error_reason() ::
    duplicate_credential_name
    | credential_in_use
    | completion_profile_credential_type_mismatch
    | credential_not_found.

-type update_completion_profiles_error_reason() ::
    credential_not_found
    | duplicate_completion_profile_name
    | completion_profile_credential_not_found
    | completion_profile_credential_type_mismatch
    | completion_profile_not_found.

-type update_credentials_error() ::
    #{reason => update_credentials_error_reason(), _ => _}.
-type update_completion_profiles_error() ::
    #{reason => update_completion_profiles_error_reason(), _ => _}.

-export_type([
    ai_type/0,
    credential/0,
    completion_profile/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Load and unload

-spec load() -> ok.
load() ->
    ok = emqx_config_handler:add_handler(?CREDENTIAL_CONFIG_PATH, ?MODULE),
    ok = emqx_config_handler:add_handler(?COMPLETION_PROFILE_CONFIG_PATH, ?MODULE),
    ok = cache_credentials(),
    ok = cache_completion_profiles().

-spec unload() -> ok.
unload() ->
    _ = persistent_term:erase(?CREDENTIAL_PT_KEY),
    _ = persistent_term:erase(?COMPLETION_PROFILE_PT_KEY),
    ok = emqx_config_handler:remove_handler(?CREDENTIAL_CONFIG_PATH),
    ok = emqx_config_handler:remove_handler(?COMPLETION_PROFILE_CONFIG_PATH).

%% Config handler callbacks

-spec pre_config_update
    (config_path(), update_credentials_request(), emqx_config:raw_config()) ->
        {ok, emqx_config:raw_config()} | {error, update_credentials_error()};
    (config_path(), update_completion_profiles_request(), emqx_config:raw_config()) ->
        {ok, emqx_config:raw_config()} | {error, update_completion_profiles_error()}.
%%
%% Credential update
%%
pre_config_update(
    ?CREDENTIAL_CONFIG_PATH, {add, #{<<"name">> := Name} = Credential}, OldCredentials
) ->
    case find_credential(Name, OldCredentials) of
        {error, #{reason := credential_not_found}} ->
            {ok, OldCredentials ++ [Credential]};
        {ok, _} ->
            {error, #{
                reason => duplicate_credential_name,
                credential => Name
            }}
    end;
pre_config_update(
    ?CREDENTIAL_CONFIG_PATH, {update, #{<<"name">> := Name} = Credential}, OldCredentials
) ->
    maybe
        {ok, OldCredential, NewCredentials} ?= update_credential(Name, Credential, OldCredentials),
        ok ?= validate_credential_update(OldCredential, Credential),
        {ok, NewCredentials}
    end;
pre_config_update(?CREDENTIAL_CONFIG_PATH, {delete, Name}, OldCredentials) ->
    maybe
        {ok, OldCredential, NewCredentials} ?= remove_credential(Name, OldCredentials),
        ok ?= validate_credential_not_used(OldCredential),
        {ok, NewCredentials}
    end;
pre_config_update(?CREDENTIAL_CONFIG_PATH, NewCredentials, _OldCredentials) when
    is_list(NewCredentials)
->
    validate_credential_presence(NewCredentials, get_completion_profiles_raw());
%%
%% Completion profile update
%%
pre_config_update(
    ?COMPLETION_PROFILE_CONFIG_PATH, {add, #{<<"name">> := Name} = Profile}, OldProfiles
) ->
    case find_completion_profile(Name, OldProfiles) of
        {error, #{reason := completion_profile_not_found}} ->
            {ok, OldProfiles ++ [Profile]};
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
    validate_credential_presence(get_credentials_raw(), NewProfiles);
pre_config_update(_Path, Request, _OldConf) ->
    {error, #{
        reason => bad_config,
        config => Request
    }}.

post_config_update(?CREDENTIAL_CONFIG_PATH, _Request, NewConf, _OldConf, _AppEnvs) ->
    cache_credentials(NewConf);
post_config_update(?COMPLETION_PROFILE_CONFIG_PATH, _Request, NewConf, _OldConf, _AppEnvs) ->
    cache_completion_profiles(NewConf).

%% Config accessors

-spec get_credential(credential_name()) -> {ok, credential()} | not_found.
get_credential(Name) ->
    case persistent_term:get(?CREDENTIAL_PT_KEY, #{}) of
        #{Name := Credential} ->
            {ok, Credential};
        _ ->
            not_found
    end.

-spec get_completion_profile(completion_profile_name()) -> {ok, completion_profile()} | not_found.
get_completion_profile(Name) ->
    case persistent_term:get(?COMPLETION_PROFILE_PT_KEY, #{}) of
        #{Name := #{credential := CredentialName} = CompletionProfile} ->
            maybe
                {ok, Credential} ?= get_credential(CredentialName),
                {ok, CompletionProfile#{credential => Credential}}
            end;
        _ ->
            not_found
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Cache management

cache_credentials() ->
    cache_credentials(emqx_config:get(?CREDENTIAL_CONFIG_PATH, [])).

cache_credentials(Config) ->
    Credentials = maps:from_list(
        lists:map(
            fun(#{name := Name} = Credential) ->
                {Name, Credential}
            end,
            Config
        )
    ),
    persistent_term:put(?CREDENTIAL_PT_KEY, Credentials).

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

%% Credential validations

validate_credential_update(#{<<"type">> := Type} = _Old, #{<<"type">> := Type} = _New) ->
    ok;
validate_credential_update(OldCredential, _NewCredential) ->
    validate_credential_not_used(OldCredential).

validate_credential_not_used(#{<<"name">> := Name} = _Credential) ->
    UsingProfiles = lists:filter(
        fun(#{<<"credential">> := N}) -> N =:= Name end,
        get_completion_profiles_raw()
    ),
    case UsingProfiles of
        [] ->
            ok;
        _ ->
            {error, #{
                reason => credential_in_use,
                credential => Name
            }}
    end.

%% Completion profile validations

validate_credential_presence(Credentials, CompletionProfiles) ->
    try
        lists:foreach(
            fun(CompletionProfile) ->
                case validate_credential_presence_for_profile(CompletionProfile, Credentials) of
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

validate_credential_presence_for_profile(
    #{<<"credential">> := Name, <<"type">> := Type, <<"name">> := ProfileName}, Credentials
) ->
    case find_credential(Name, Credentials) of
        {ok, #{<<"type">> := Type}} ->
            ok;
        {ok, #{<<"type">> := OtherType}} ->
            {error, #{
                reason => completion_profile_credential_type_mismatch,
                profile_name => ProfileName,
                profile_type => Type,
                credential => Name,
                credential_type => OtherType
            }};
        {error, not_found} ->
            {error, #{
                reason => completion_profile_credential_not_found,
                profile_name => ProfileName,
                credential => Name
            }}
    end.

validate_completion_profile_update(CompletionProfile) ->
    validate_credential_presence_for_profile(CompletionProfile, get_credentials_raw()).

%% Completion profile management

get_completion_profiles_raw() ->
    emqx_config:get_raw([ai, completion_profiles], []).

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

%% Credential management

get_credentials_raw() ->
    emqx_config:get_raw([ai, credentials], []).

find_credential(Name, Credentials) ->
    wrap_credential_not_found(
        Name,
        find_by_key(<<"name">>, Name, Credentials)
    ).

remove_credential(Name, Credentials) ->
    wrap_credential_not_found(
        Name,
        remove_by_key(<<"name">>, Name, Credentials)
    ).

update_credential(Name, Credential, Credentials) ->
    wrap_credential_not_found(
        Name,
        update_by_key(<<"name">>, Name, Credential, Credentials)
    ).

wrap_credential_not_found(Name, not_found) ->
    {error, #{
        reason => credential_not_found,
        credential => Name
    }};
wrap_credential_not_found(_Name, Result) ->
    Result.

%% Helpers

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
