%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_config).

-export([
    load/0,
    update/1,
    max_sessions_per_username/0,
    snapshot_refresh_interval_ms/0,
    snapshot_request_timeout_ms/0,
    is_whitelisted/1,
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
    persistent_term:put(?SETTINGS_KEY, parse(RawConfig)),
    ok.

max_sessions_per_username() ->
    maps:get(max_sessions_per_username, settings(), ?DEFAULT_MAX_SESSIONS_PER_USERNAME).

snapshot_refresh_interval_ms() ->
    maps:get(snapshot_refresh_interval_ms, settings(), ?DEFAULT_SNAPSHOT_REFRESH_INTERVAL_MS).

snapshot_request_timeout_ms() ->
    maps:get(snapshot_request_timeout_ms, settings(), ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS).

is_whitelisted(Username) when is_binary(Username) ->
    WhiteList = maps:get(username_white_list, settings(), #{}),
    maps:is_key(Username, WhiteList).

settings() ->
    persistent_term:get(?SETTINGS_KEY, default_settings()).

parse(RawConfig) when is_map(RawConfig) ->
    Max0 = get_value(
        RawConfig,
        [max_sessions_per_username, <<"max_sessions_per_username">>],
        ?DEFAULT_MAX_SESSIONS_PER_USERNAME
    ),
    Max = normalize_max(Max0),
    WhiteList0 = get_value(
        RawConfig,
        [username_white_list, <<"username_white_list">>],
        []
    ),
    WhiteList = normalize_whitelist(WhiteList0),
    RefreshMs0 = get_value(
        RawConfig,
        [snapshot_refresh_interval_ms, <<"snapshot_refresh_interval_ms">>],
        ?DEFAULT_SNAPSHOT_REFRESH_INTERVAL_MS
    ),
    RequestTimeoutMs0 = get_value(
        RawConfig,
        [snapshot_request_timeout_ms, <<"snapshot_request_timeout_ms">>],
        ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
    ),
    #{
        max_sessions_per_username => Max,
        username_white_list => WhiteList,
        snapshot_refresh_interval_ms => normalize_ms(
            RefreshMs0, ?DEFAULT_SNAPSHOT_REFRESH_INTERVAL_MS
        ),
        snapshot_request_timeout_ms => normalize_ms(
            RequestTimeoutMs0, ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
        )
    };
parse(_RawConfig) ->
    default_settings().

default_settings() ->
    #{
        max_sessions_per_username => ?DEFAULT_MAX_SESSIONS_PER_USERNAME,
        username_white_list => #{},
        snapshot_refresh_interval_ms => ?DEFAULT_SNAPSHOT_REFRESH_INTERVAL_MS,
        snapshot_request_timeout_ms => ?DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS
    }.

normalize_max(Value) when is_integer(Value), Value > 0 ->
    Value;
normalize_max(Value) when is_binary(Value) ->
    case binary_to_integer_safe(Value) of
        Int when is_integer(Int), Int > 0 -> Int;
        _ -> ?DEFAULT_MAX_SESSIONS_PER_USERNAME
    end;
normalize_max(Value) when is_list(Value) ->
    normalize_max(iolist_to_binary(Value));
normalize_max(_) ->
    ?DEFAULT_MAX_SESSIONS_PER_USERNAME.

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

normalize_whitelist(List) when is_list(List) ->
    maps:from_list(
        lists:filtermap(
            fun(Entry) ->
                case normalize_whitelist_entry(Entry) of
                    <<>> -> false;
                    Username -> {true, {Username, true}}
                end
            end,
            List
        )
    );
normalize_whitelist(_) ->
    #{}.

normalize_whitelist_entry(#{username := Username}) ->
    to_bin(Username);
normalize_whitelist_entry(#{<<"username">> := Username}) ->
    to_bin(Username);
normalize_whitelist_entry(Username) ->
    to_bin(Username).

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

to_bin(Value) when is_binary(Value) ->
    Value;
to_bin(Value) when is_list(Value) ->
    try iolist_to_binary(Value) of
        Bin -> Bin
    catch
        _:_ -> <<>>
    end;
to_bin(Value) when is_integer(Value) ->
    integer_to_binary(Value);
to_bin(_) ->
    <<>>.

plugin_name_vsn() ->
    App = <<"emqx_username_quota">>,
    Vsn =
        case application:get_key(emqx_username_quota, vsn) of
            {ok, V} when is_list(V) -> unicode:characters_to_binary(V);
            {ok, V} when is_binary(V) -> V;
            _ -> <<"1.0.0">>
        end,
    <<App/binary, "-", Vsn/binary>>.
