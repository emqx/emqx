%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_config).

-export([
    load/0,
    update/1,
    max_sessions_per_username/0,
    snapshot_min_age_ms/0,
    snapshot_request_timeout_ms/0,
    settings/0
]).

-include("emqx_username_quota.hrl").

-define(SETTINGS_KEY, {?MODULE, settings}).

load() ->
    NameVsn = plugin_name_vsn(),
    Raw =
        try emqx_plugins:get_config(NameVsn, #{}) of
            Config -> Config
        catch
            _:_ -> #{}
        end,
    update(Raw).

update(RawConfig) ->
    case parse(RawConfig) of
        {error, _} = Error ->
            Error;
        Settings when is_map(Settings) ->
            persistent_term:put(?SETTINGS_KEY, Settings),
            ok
    end.

max_sessions_per_username() ->
    maps:get(max_sessions_per_username, settings(), ?DEFAULT_MAX_SESSIONS_PER_USERNAME).

snapshot_min_age_ms() ->
    maps:get(snapshot_min_age_ms, settings(), ?DEFAULT_SNAPSHOT_MIN_AGE_MS).

snapshot_request_timeout_ms() ->
    maps:get(snapshot_request_timeout_ms, settings(), ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS).

settings() ->
    persistent_term:get(?SETTINGS_KEY, default_settings()).

parse(RawConfig) when is_map(RawConfig) ->
    Max0 = get_value(
        RawConfig,
        [max_sessions_per_username, <<"max_sessions_per_username">>],
        ?DEFAULT_MAX_SESSIONS_PER_USERNAME
    ),
    case normalize_max(Max0) of
        {ok, Max} ->
            MinAgeMs0 = get_value(
                RawConfig,
                [snapshot_min_age_ms, <<"snapshot_min_age_ms">>],
                ?DEFAULT_SNAPSHOT_MIN_AGE_MS
            ),
            RequestTimeoutMs0 = get_value(
                RawConfig,
                [snapshot_request_timeout_ms, <<"snapshot_request_timeout_ms">>],
                ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
            ),
            #{
                max_sessions_per_username => Max,
                snapshot_min_age_ms => normalize_min_age_ms(
                    normalize_ms(MinAgeMs0, ?DEFAULT_SNAPSHOT_MIN_AGE_MS)
                ),
                snapshot_request_timeout_ms => normalize_ms(
                    RequestTimeoutMs0, ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
                )
            };
        {error, _} = Error ->
            Error
    end;
parse(_RawConfig) ->
    default_settings().

default_settings() ->
    #{
        max_sessions_per_username => ?DEFAULT_MAX_SESSIONS_PER_USERNAME,
        snapshot_min_age_ms => ?DEFAULT_SNAPSHOT_MIN_AGE_MS,
        snapshot_request_timeout_ms => ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
    }.

normalize_max(Value) when is_integer(Value), Value > 0 ->
    {ok, Value};
normalize_max(Value) when is_integer(Value) ->
    {error, {invalid_max_sessions_per_username, Value}};
normalize_max(Value) when is_binary(Value) ->
    case binary_to_integer_safe(Value) of
        Int when is_integer(Int) -> normalize_max(Int);
        _ -> {error, {invalid_max_sessions_per_username, Value}}
    end;
normalize_max(Value) when is_list(Value) ->
    normalize_max(iolist_to_binary(Value));
normalize_max(Value) ->
    {error, {invalid_max_sessions_per_username, Value}}.

normalize_min_age_ms(Value) when is_integer(Value), Value < ?MIN_SNAPSHOT_MIN_AGE_MS ->
    ?MIN_SNAPSHOT_MIN_AGE_MS;
normalize_min_age_ms(Value) when is_integer(Value), Value > ?MAX_SNAPSHOT_MIN_AGE_MS ->
    ?MAX_SNAPSHOT_MIN_AGE_MS;
normalize_min_age_ms(Value) when is_integer(Value) ->
    Value.

normalize_ms(Value, _Default) when is_integer(Value), Value > 0 ->
    Value;
normalize_ms(Value, Default) when is_binary(Value) ->
    case binary_to_integer_safe(Value) of
        Int when is_integer(Int), Int > 0 -> Int;
        _ -> Default
    end;
normalize_ms(Value, Default) when is_list(Value) ->
    normalize_ms(iolist_to_binary(Value), Default);
normalize_ms(_Value, Default) ->
    Default.

get_value(Map, [K | Ks], Default) ->
    case maps:find(K, Map) of
        {ok, V} -> V;
        error -> get_value(Map, Ks, Default)
    end;
get_value(_Map, [], Default) ->
    Default.

binary_to_integer_safe(Bin) ->
    try binary_to_integer(Bin) of
        Int -> Int
    catch
        _:_ -> invalid
    end.

plugin_name_vsn() ->
    App = <<"emqx_username_quota">>,
    Vsn =
        case application:get_key(emqx_username_quota, vsn) of
            {ok, V} when is_list(V) -> unicode:characters_to_binary(V);
            {ok, V} when is_binary(V) -> V;
            _ -> <<"1.0.0">>
        end,
    <<App/binary, "-", Vsn/binary>>.
