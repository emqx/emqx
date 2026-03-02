%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_config).

-include_lib("kernel/include/logger.hrl").

-export([load/0, update/1, get_bridges/0]).

-define(SETTINGS_KEY, {?MODULE, settings}).

load() ->
    NameVsn = name_vsn(),
    Raw = emqx_plugins:get_config(NameVsn, #{}),
    update(Raw).

update(RawConfig) ->
    Parsed = parse(RawConfig),
    persistent_term:put(?SETTINGS_KEY, Parsed),
    ok.

-spec get_bridges() -> [map()].
get_bridges() ->
    Settings = settings(),
    maps:get(bridges, Settings, []).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

settings() ->
    persistent_term:get(?SETTINGS_KEY, #{bridges => []}).

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_bridge_mqtt_dq, vsn),
    iolist_to_binary([<<"emqx_bridge_mqtt_dq-">>, Vsn]).

parse(RawConfig) when is_map(RawConfig) ->
    RawBridges = maps:get(<<"bridges">>, RawConfig, #{}),
    Bridges = parse_bridges(RawBridges),
    #{bridges => Bridges};
parse(_) ->
    #{bridges => []}.

parse_bridges(RawMap) when is_map(RawMap) ->
    maps:fold(fun fold_bridge/3, [], RawMap);
parse_bridges(_) ->
    [].

fold_bridge(Name, Raw, Acc) ->
    BinName = to_bin(Name),
    case is_valid_name(BinName) andalso parse_bridge(BinName, Raw) of
        {ok, Bridge} ->
            [Bridge | Acc];
        false ->
            ?LOG_ERROR(#{
                msg => "mqtt_dq_bridge_invalid_name",
                name => Name
            }),
            Acc;
        error ->
            ?LOG_ERROR(#{
                msg => "mqtt_dq_bridge_config_parse_error",
                name => BinName
            }),
            Acc
    end.

parse_bridge(Name, Raw) when is_map(Raw) ->
    try
        Bridge = #{
            name => Name,
            enable => to_boolean(get_val(<<"enable">>, Raw, false)),
            server => to_str(get_val(<<"server">>, Raw, "127.0.0.1:1883")),
            proto_ver => parse_proto_ver(
                get_val(<<"proto_ver">>, Raw, <<"v4">>)
            ),
            clientid_prefix => to_bin(
                get_val(<<"clientid_prefix">>, Raw, Name)
            ),
            username => to_bin(get_val(<<"username">>, Raw, <<>>)),
            password => to_bin(get_val(<<"password">>, Raw, <<>>)),
            clean_start => to_boolean(get_val(<<"clean_start">>, Raw, true)),
            keepalive_s => to_pos_int(
                get_val(<<"keepalive_s">>, Raw, 60)
            ),
            ssl => parse_ssl(get_val(<<"ssl">>, Raw, #{})),
            pool_size => to_pos_int(get_val(<<"pool_size">>, Raw, 4)),
            buffer_pool_size => to_pos_int(
                get_val(<<"buffer_pool_size">>, Raw, 4)
            ),
            filter_topic => to_bin(
                get_val(<<"filter_topic">>, Raw, <<"#">>)
            ),
            remote_topic => to_bin(
                get_val(<<"remote_topic">>, Raw, <<"fwd/${topic}">>)
            ),
            enqueue_timeout_ms => to_pos_int(
                get_val(<<"enqueue_timeout_ms">>, Raw, 5000)
            ),
            remote_qos => to_qos(get_val(<<"remote_qos">>, Raw, 1)),
            remote_retain => to_boolean(
                get_val(<<"remote_retain">>, Raw, false)
            ),
            queue_dir => parse_queue_dir(get_val(<<"queue">>, Raw, #{}), Name),
            seg_bytes => parse_bytes(
                get_nested(
                    [<<"queue">>, <<"seg_bytes">>], Raw, <<"100MB">>
                )
            ),
            max_total_bytes => parse_bytes(
                get_nested(
                    [<<"queue">>, <<"max_total_bytes">>],
                    Raw,
                    <<"1GB">>
                )
            )
        },
        {ok, Bridge}
    catch
        _:_ -> error
    end;
parse_bridge(_Name, _) ->
    error.

is_valid_name(<<>>) ->
    false;
is_valid_name(Name) ->
    re:run(Name, <<"^[A-Za-z0-9_-]+$">>, [{capture, none}]) =:= match.

get_val(Key, Map, Default) ->
    maps:get(Key, Map, Default).

get_nested(Keys, Map, Default) ->
    get_nested_val(Keys, Map, Default).

get_nested_val([], _Map) ->
    undefined;
get_nested_val([Key], Map) when is_map(Map) ->
    maps:get(Key, Map, undefined);
get_nested_val([Key | Rest], Map) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} -> get_nested_val(Rest, SubMap);
        error -> undefined
    end;
get_nested_val(_, _) ->
    undefined.

get_nested_val(Keys, Map, Default) ->
    case get_nested_val(Keys, Map) of
        undefined -> Default;
        Val -> Val
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_integer(V) -> integer_to_binary(V).

to_str(V) when is_list(V) -> V;
to_str(V) when is_binary(V) -> binary_to_list(V).

to_boolean(true) -> true;
to_boolean(false) -> false;
to_boolean(<<"true">>) -> true;
to_boolean(<<"false">>) -> false;
to_boolean(_) -> false.

to_pos_int(V) when is_integer(V), V > 0 -> V;
to_pos_int(V) when is_binary(V) -> to_pos_int(binary_to_integer(V));
to_pos_int(_) -> 4.

to_qos(0) -> 0;
to_qos(1) -> 1;
to_qos(2) -> 2;
to_qos(V) when is_binary(V) -> to_qos(binary_to_integer(V));
to_qos(_) -> 1.

parse_proto_ver(<<"v3">>) -> v3;
parse_proto_ver(<<"v4">>) -> v4;
parse_proto_ver(<<"v5">>) -> v5;
parse_proto_ver(_) -> v4.

parse_ssl(#{} = Raw) ->
    #{
        enable => to_boolean(get_val(<<"enable">>, Raw, false)),
        server_name_indication => parse_sni(
            get_val(<<"sni">>, Raw, undefined)
        )
    };
parse_ssl(_) ->
    #{enable => false, server_name_indication => undefined}.

parse_sni(undefined) -> undefined;
parse_sni(<<"disable">>) -> disable;
parse_sni(V) when is_binary(V) -> binary_to_list(V);
parse_sni(V) when is_list(V) -> V;
parse_sni(_) -> undefined.

parse_queue_dir(#{} = QueueConf, BridgeName) ->
    Default = iolist_to_binary([<<"data/bridge_mqtt_dq/">>, BridgeName]),
    to_bin(get_val(<<"dir">>, QueueConf, Default));
parse_queue_dir(_, BridgeName) ->
    iolist_to_binary([<<"data/bridge_mqtt_dq/">>, BridgeName]).

parse_bytes(V) when is_integer(V) -> V;
parse_bytes(V) when is_binary(V) ->
    case re:run(V, <<"^([0-9]+)(KB|MB|GB|kb|mb|gb)?$">>, [{capture, all_but_first, binary}]) of
        {match, [Num, <<"KB">>]} -> binary_to_integer(Num) * 1024;
        {match, [Num, <<"kb">>]} -> binary_to_integer(Num) * 1024;
        {match, [Num, <<"MB">>]} -> binary_to_integer(Num) * 1024 * 1024;
        {match, [Num, <<"mb">>]} -> binary_to_integer(Num) * 1024 * 1024;
        {match, [Num, <<"GB">>]} -> binary_to_integer(Num) * 1024 * 1024 * 1024;
        {match, [Num, <<"gb">>]} -> binary_to_integer(Num) * 1024 * 1024 * 1024;
        {match, [Num]} -> binary_to_integer(Num);
        _ -> binary_to_integer(V)
    end;
parse_bytes(_) ->
    %% 100MB default
    104857600.
