%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_config).

-moduledoc """
Loads and exposes runtime plugin settings for UNS Governance.
""".

-export([
    load/0,
    update/1,
    settings/0,
    on_mismatch/0,
    validate_payload/0,
    exempt_topics/0,
    is_exempt_topic/1
]).

-include("emqx_unsgov.hrl").

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
            {error, {bad_plugin_config, Reason}}
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

on_mismatch() ->
    maps:get(on_mismatch, settings(), ?DEFAULT_ON_MISMATCH).

validate_payload() ->
    maps:get(validate_payload, settings(), ?DEFAULT_VALIDATE_PAYLOAD).

exempt_topics() ->
    maps:get(exempt_topics, settings(), ?DEFAULT_EXEMPT_TOPICS).

is_exempt_topic(Topic) when is_binary(Topic) ->
    emqx_topic:match_any(Topic, exempt_topics()).

parse(RawConfig) when is_map(RawConfig) ->
    maybe
        {ok, OnMismatch} ?= check_on_mismatch(RawConfig),
        {ok, ValidatePayload} ?=
            check_bool(<<"validate_payload">>, RawConfig, ?DEFAULT_VALIDATE_PAYLOAD),
        {ok, ExemptTopics} ?= check_exempt_topics(RawConfig),
        {ok, #{
            on_mismatch => OnMismatch,
            validate_payload => ValidatePayload,
            exempt_topics => ExemptTopics
        }}
    end;
parse(Config) ->
    {error, #{cause => invalid_config, value => Config}}.

default_settings() ->
    #{
        on_mismatch => ?DEFAULT_ON_MISMATCH,
        validate_payload => ?DEFAULT_VALIDATE_PAYLOAD,
        exempt_topics => ?DEFAULT_EXEMPT_TOPICS
    }.

check_bool(Key, Config, Default) ->
    case maps:get(Key, Config, Default) of
        V when is_boolean(V) -> {ok, V};
        V -> {error, #{cause => invalid_value, key => Key, value => V}}
    end.

check_on_mismatch(Config) ->
    case maps:get(<<"on_mismatch">>, Config, ?DEFAULT_ON_MISMATCH) of
        <<"deny">> -> {ok, deny};
        <<"ignore">> -> {ok, ignore};
        deny -> {ok, deny};
        ignore -> {ok, ignore};
        V -> {error, #{cause => invalid_value, key => <<"on_mismatch">>, value => V}}
    end.

check_exempt_topics(Config) ->
    case maps:get(<<"exempt_topics">>, Config, ?DEFAULT_EXEMPT_TOPICS) of
        List when is_list(List) ->
            case lists:all(fun(V) -> is_binary(V) andalso V =/= <<>> end, List) of
                true ->
                    {ok, List};
                false ->
                    {error, #{cause => invalid_value, key => <<"exempt_topics">>, value => List}}
            end;
        V ->
            {error, #{cause => invalid_value, key => <<"exempt_topics">>, value => V}}
    end.

plugin_name_vsn() ->
    App = <<"emqx_unsgov">>,
    {ok, Vsn} = application:get_key(emqx_unsgov, vsn),
    iolist_to_binary([App, "-", Vsn]).
