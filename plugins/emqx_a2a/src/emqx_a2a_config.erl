%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_config).

-moduledoc """
Loads and exposes runtime plugin settings for A2A orchestration.
""".

-export([
    load/0,
    update/1,
    settings/0,
    request_topic_prefix/0,
    provider/0,
    default_model/0,
    default_max_tokens/0,
    api_key/0
]).

-include("emqx_a2a.hrl").

-define(SETTINGS_KEY, {?MODULE, settings}).

load() ->
    NameVsn = plugin_name_vsn(),
    Raw =
        try emqx_plugins:get_config(NameVsn, #{}) of
            Config -> Config
        catch
            _:_ -> #{}
        end,
    case parse(Raw) of
        {ok, Settings} ->
            persistent_term:put(?SETTINGS_KEY, Settings),
            ok;
        {error, Reason} ->
            ?LOG(warning, #{msg => "a2a_config_error", reason => Reason}),
            persistent_term:put(?SETTINGS_KEY, default_settings()),
            ok
    end.

update(RawConfig) ->
    case parse(RawConfig) of
        {ok, Settings} ->
            persistent_term:put(?SETTINGS_KEY, Settings),
            ok;
        {error, Reason} ->
            ?LOG(error, #{msg => "bad_plugin_config", reason => Reason}),
            ok
    end.

settings() ->
    persistent_term:get(?SETTINGS_KEY, default_settings()).

request_topic_prefix() ->
    maps:get(request_topic_prefix, settings(), ?DEFAULT_REQUEST_TOPIC_PREFIX).

provider() ->
    maps:get(provider, settings(), ?DEFAULT_PROVIDER).

default_model() ->
    maps:get(default_model, settings(), ?DEFAULT_MODEL).

default_max_tokens() ->
    maps:get(default_max_tokens, settings(), ?DEFAULT_MAX_TOKENS).

api_key() ->
    maps:get(api_key, settings(), <<>>).

parse(RawConfig) when is_map(RawConfig) ->
    maybe
        {ok, Prefix} ?=
            check_binary(
                <<"request_topic_prefix">>,
                RawConfig,
                ?DEFAULT_REQUEST_TOPIC_PREFIX
            ),
        {ok, Provider} ?= check_provider(RawConfig),
        {ok, Model} ?= check_binary(<<"default_model">>, RawConfig, ?DEFAULT_MODEL),
        {ok, MaxTokens} ?=
            check_pos_integer(
                <<"default_max_tokens">>,
                RawConfig,
                ?DEFAULT_MAX_TOKENS
            ),
        ApiKey = maps:get(<<"api_key">>, RawConfig, <<>>),
        {ok, #{
            request_topic_prefix => Prefix,
            provider => Provider,
            default_model => Model,
            default_max_tokens => MaxTokens,
            api_key => emqx_utils_conv:bin(ApiKey)
        }}
    end;
parse(Config) ->
    {error, #{cause => invalid_config, value => Config}}.

default_settings() ->
    #{
        request_topic_prefix => ?DEFAULT_REQUEST_TOPIC_PREFIX,
        provider => ?DEFAULT_PROVIDER,
        default_model => ?DEFAULT_MODEL,
        default_max_tokens => ?DEFAULT_MAX_TOKENS,
        api_key => <<>>
    }.

check_binary(Key, Config, Default) ->
    case maps:get(Key, Config, Default) of
        V when is_binary(V) -> {ok, V};
        V when is_list(V) -> {ok, unicode:characters_to_binary(V)};
        V -> {error, #{cause => invalid_value, key => Key, value => V}}
    end.

check_pos_integer(Key, Config, Default) ->
    case maps:get(Key, Config, Default) of
        V when is_integer(V), V > 0 -> {ok, V};
        V -> {error, #{cause => invalid_value, key => Key, value => V}}
    end.

check_provider(Config) ->
    case maps:get(<<"provider">>, Config, ?DEFAULT_PROVIDER) of
        <<"openai">> -> {ok, openai};
        <<"anthropic">> -> {ok, anthropic};
        openai -> {ok, openai};
        anthropic -> {ok, anthropic};
        V -> {error, #{cause => invalid_value, key => <<"provider">>, value => V}}
    end.

plugin_name_vsn() ->
    App = <<"emqx_a2a">>,
    {ok, Vsn} = application:get_key(emqx_a2a, vsn),
    iolist_to_binary([App, "-", Vsn]).
