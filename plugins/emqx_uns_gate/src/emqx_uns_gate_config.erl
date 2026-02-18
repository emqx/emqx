%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_config).

-export([
    load/0,
    update/1,
    settings/0,
    enabled/0,
    on_mismatch/0,
    allow_intermediate_publish/0,
    validate_payload/0,
    exempt_topics/0,
    is_exempt_topic/1
]).

-include("emqx_uns_gate.hrl").

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

settings() ->
    persistent_term:get(?SETTINGS_KEY, default_settings()).

enabled() ->
    maps:get(enabled, settings(), ?DEFAULT_ENABLED).

on_mismatch() ->
    maps:get(on_mismatch, settings(), ?DEFAULT_ON_MISMATCH).

allow_intermediate_publish() ->
    maps:get(allow_intermediate_publish, settings(), ?DEFAULT_ALLOW_INTERMEDIATE_PUBLISH).

validate_payload() ->
    maps:get(validate_payload, settings(), ?DEFAULT_VALIDATE_PAYLOAD).

exempt_topics() ->
    maps:get(exempt_topics, settings(), ?DEFAULT_EXEMPT_TOPICS).

is_exempt_topic(Topic) when is_binary(Topic) ->
    lists:any(fun(Filter) -> emqx_topic:match(Topic, Filter) end, exempt_topics()).

parse(RawConfig) when is_map(RawConfig) ->
    #{
        enabled => normalize_bool(
            get_value(RawConfig, [enabled, <<"enabled">>], ?DEFAULT_ENABLED),
            ?DEFAULT_ENABLED
        ),
        on_mismatch => normalize_on_mismatch(
            get_value(RawConfig, [on_mismatch, <<"on_mismatch">>], ?DEFAULT_ON_MISMATCH)
        ),
        allow_intermediate_publish => normalize_bool(
            get_value(
                RawConfig,
                [allow_intermediate_publish, <<"allow_intermediate_publish">>],
                ?DEFAULT_ALLOW_INTERMEDIATE_PUBLISH
            ),
            ?DEFAULT_ALLOW_INTERMEDIATE_PUBLISH
        ),
        validate_payload => normalize_bool(
            get_value(
                RawConfig,
                [validate_payload, <<"validate_payload">>],
                ?DEFAULT_VALIDATE_PAYLOAD
            ),
            ?DEFAULT_VALIDATE_PAYLOAD
        ),
        exempt_topics => normalize_exempt_topics(
            get_value(RawConfig, [exempt_topics, <<"exempt_topics">>], ?DEFAULT_EXEMPT_TOPICS)
        )
    };
parse(_) ->
    default_settings().

default_settings() ->
    #{
        enabled => ?DEFAULT_ENABLED,
        on_mismatch => ?DEFAULT_ON_MISMATCH,
        allow_intermediate_publish => ?DEFAULT_ALLOW_INTERMEDIATE_PUBLISH,
        validate_payload => ?DEFAULT_VALIDATE_PAYLOAD,
        exempt_topics => ?DEFAULT_EXEMPT_TOPICS
    }.

normalize_bool(true, _) -> true;
normalize_bool(false, _) -> false;
normalize_bool(<<"true">>, _) -> true;
normalize_bool(<<"false">>, _) -> false;
normalize_bool("true", _) -> true;
normalize_bool("false", _) -> false;
normalize_bool(_, Default) -> Default.

normalize_on_mismatch(deny) -> deny;
normalize_on_mismatch(ignore) -> ignore;
normalize_on_mismatch(<<"deny">>) -> deny;
normalize_on_mismatch(<<"ignore">>) -> ignore;
normalize_on_mismatch("deny") -> deny;
normalize_on_mismatch("ignore") -> ignore;
normalize_on_mismatch(_) -> ?DEFAULT_ON_MISMATCH.

normalize_exempt_topics(List) when is_list(List) ->
    Topics = lists:filtermap(
        fun
            (V) when is_binary(V), V =/= <<>> -> {true, V};
            (V) when is_list(V), V =/= [] -> {true, unicode:characters_to_binary(V)};
            (_) -> false
        end,
        List
    ),
    case Topics of
        [] -> ?DEFAULT_EXEMPT_TOPICS;
        _ -> Topics
    end;
normalize_exempt_topics(_) ->
    ?DEFAULT_EXEMPT_TOPICS.

get_value(Map, [K | Ks], Default) ->
    case maps:find(K, Map) of
        {ok, V} -> V;
        error -> get_value(Map, Ks, Default)
    end;
get_value(_Map, [], Default) ->
    Default.

plugin_name_vsn() ->
    App = <<"emqx_uns_gate">>,
    Vsn =
        case application:get_key(emqx_uns_gate, vsn) of
            {ok, V} when is_list(V) -> unicode:characters_to_binary(V);
            {ok, V} when is_binary(V) -> V;
            _ -> <<"1.0.0">>
        end,
    <<App/binary, "-", Vsn/binary>>.
