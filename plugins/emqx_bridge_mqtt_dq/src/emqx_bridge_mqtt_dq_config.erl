%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_config).

-include("emqx_bridge_mqtt_dq.hrl").

-export([load/0, update/1, get_bridges/0]).

-define(SETTINGS_KEY, {?MODULE, settings}).

load() ->
    NameVsn = name_vsn(),
    Raw = emqx_plugins:get_config(NameVsn, #{}),
    update(Raw).

update(RawConfig) ->
    case parse(RawConfig) of
        {ok, Parsed} ->
            persistent_term:put(?SETTINGS_KEY, Parsed),
            ok;
        {error, _} = Error ->
            Error
    end.

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
    RawConfig1 = resolve_envs(RawConfig),
    RawRemotes = maps:get(<<"remotes">>, RawConfig1, #{}),
    RawBridges = maps:get(<<"bridges">>, RawConfig1, #{}),
    maybe
        {ok, Remotes} ?= parse_remotes(RawRemotes),
        {ok, Bridges} ?= parse_bridges(RawBridges, Remotes),
        {ok, #{bridges => Bridges}}
    end.

parse_remotes(RawMap) when is_map(RawMap) ->
    maps:fold(fun fold_remote/3, {ok, #{}}, RawMap).

parse_bridges(RawMap, Remotes) when is_map(RawMap) ->
    maps:fold(fun(Name, Raw, Acc) -> fold_bridge(Name, Raw, Remotes, Acc) end, {ok, []}, RawMap).

fold_remote(_Name, _Raw, {error, _} = Err) ->
    Err;
fold_remote(Name, Raw, {ok, Acc}) ->
    BinName = to_bin(Name),
    case is_valid_name(BinName) of
        false ->
            {error,
                fmt(
                    "remotes.~ts: invalid name, "
                    "must match [A-Za-z0-9_-]+",
                    [Name]
                )};
        true ->
            case parse_remote(Raw) of
                {ok, Remote} ->
                    {ok, Acc#{BinName => Remote}};
                {error, Reason} ->
                    {error, fmt("remotes.~ts: ~ts", [BinName, format_reason(Reason)])}
            end
    end.

fold_bridge(_Name, _Raw, _Remotes, {error, _} = Err) ->
    Err;
fold_bridge(Name, Raw, Remotes, {ok, Acc}) ->
    BinName = to_bin(Name),
    case is_valid_name(BinName) of
        false ->
            {error,
                fmt(
                    "bridges.~ts: invalid name, "
                    "must match [A-Za-z0-9_-]+",
                    [Name]
                )};
        true ->
            case parse_bridge(BinName, Raw, Remotes) of
                {ok, Bridge} ->
                    {ok, [Bridge | Acc]};
                {error, Reason} ->
                    {error, fmt("bridges.~ts: ~ts", [BinName, format_reason(Reason)])}
            end
    end.

parse_remote(Raw) when is_map(Raw) ->
    try
        {ok, #{
            server => to_str(get_val(<<"server">>, Raw, "127.0.0.1:1883")),
            username => to_bin(get_val(<<"username">>, Raw, <<>>)),
            password => to_bin_or_empty(get_val(<<"password">>, Raw, <<>>)),
            ssl => parse_ssl(get_val(<<"ssl">>, Raw, #{}))
        }}
    catch
        _:Reason -> {error, Reason}
    end;
parse_remote(Raw) ->
    {error, {expected_map, Raw}}.

parse_bridge(Name, Raw, Remotes) when is_map(Raw) ->
    try
        Remote = resolve_remote(Raw, Remotes),
        Bridge = #{
            name => Name,
            enable => to_boolean(get_val(<<"enable">>, Raw, false)),
            server => maps:get(server, Remote),
            proto_ver => parse_proto_ver(
                get_val(<<"proto_ver">>, Raw, <<"v4">>)
            ),
            clientid_prefix => parse_clientid_prefix(
                get_val(<<"clientid_prefix">>, Raw, <<>>), Name
            ),
            username => maps:get(username, Remote),
            password => maps:get(password, Remote),
            keepalive_s => to_pos_int(
                get_val(<<"keepalive_s">>, Raw, 60)
            ),
            ssl => maps:get(ssl, Remote),
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
            max_inflight => to_pos_int(
                get_val(<<"max_inflight">>, Raw, 32)
            ),
            max_publish_retries => parse_max_publish_retries(
                get_val(<<"max_publish_retries">>, Raw, -1)
            ),
            remote_qos => parse_remote_qos(get_val(<<"remote_qos">>, Raw, <<"${qos}">>)),
            remote_retain => parse_remote_retain(
                get_val(<<"remote_retain">>, Raw, <<"${retain}">>)
            ),
            queue_base_dir => parse_queue_base_dir(get_val(<<"queue">>, Raw, #{})),
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
        _:Reason -> {error, Reason}
    end;
parse_bridge(_Name, Raw, _) ->
    {error, {expected_map, Raw}}.

resolve_remote(Raw, Remotes) ->
    case maps:find(<<"remote">>, Raw) of
        {ok, RemoteName} ->
            RemoteKey = to_bin(RemoteName),
            case RemoteKey of
                <<>> ->
                    error(
                        "'remote' is empty; set a remote name or "
                        "configure 'server' directly"
                    );
                _ ->
                    case maps:find(RemoteKey, Remotes) of
                        {ok, Remote} ->
                            Remote;
                        error ->
                            error(
                                fmt(
                                    "remote '~ts' not found in remotes config",
                                    [RemoteKey]
                                )
                            )
                    end
            end;
        error ->
            case parse_remote(Raw) of
                {ok, Remote} -> Remote;
                {error, Reason} -> error(Reason)
            end
    end.

resolve_envs(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> resolve_envs(V) end, Map);
resolve_envs(List) when is_list(List) ->
    [resolve_envs(Elem) || Elem <- List];
resolve_envs(<<"${EMQXDQ_", _/binary>> = Original) ->
    case binary:last(Original) of
        $} ->
            VarName = binary:part(Original, 2, byte_size(Original) - 3),
            resolve_env_value(VarName, Original);
        _ ->
            Original
    end;
resolve_envs(Other) ->
    Other.

resolve_env_value(VarName, Original) ->
    case os:getenv(binary_to_list(VarName)) of
        false ->
            ?LOG(error, #{
                msg => "mqtt_dq_config_env_var_not_set",
                env_var => VarName
            }),
            Original;
        Value ->
            list_to_binary(Value)
    end.

is_valid_name(<<>>) ->
    false;
is_valid_name(Name) ->
    re:run(Name, <<"^[A-Za-z0-9_-]+$">>, [{capture, none}]) =:= match.

get_val(Key, Map, Default) ->
    maps:get(Key, Map, Default).

get_nested(Keys, Map, Default) ->
    get_nested_val(Keys, Map, Default).

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

to_bin_or_empty(null) -> <<>>;
to_bin_or_empty(undefined) -> <<>>;
to_bin_or_empty(V) -> to_bin(V).

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

parse_max_publish_retries(-1) -> infinity;
parse_max_publish_retries(V) when is_integer(V), V >= 0 -> V;
parse_max_publish_retries(V) when is_binary(V) -> parse_max_publish_retries(binary_to_integer(V));
parse_max_publish_retries(_) -> infinity.

to_qos(0) -> 0;
to_qos(1) -> 1;
to_qos(2) -> 2;
to_qos(V) when is_binary(V) -> to_qos(binary_to_integer(V));
to_qos(_) -> 1.

parse_remote_qos(<<"${qos}">>) -> '${qos}';
parse_remote_qos(V) -> to_qos(V).

parse_remote_retain(<<"${retain}">>) -> '${retain}';
parse_remote_retain(V) -> to_boolean(V).

parse_proto_ver(<<"v3">>) -> v3;
parse_proto_ver(<<"v4">>) -> v4;
parse_proto_ver(<<"v5">>) -> v5;
parse_proto_ver(_) -> v4.

parse_ssl(#{} = Raw) ->
    #{
        enable => to_boolean(get_val(<<"enable">>, Raw, false)),
        verify => parse_verify(get_val(<<"verify">>, Raw, <<"verify_none">>)),
        cacertfile => parse_file_path(get_val(<<"cacertfile">>, Raw, undefined)),
        certfile => parse_file_path(get_val(<<"certfile">>, Raw, undefined)),
        keyfile => parse_file_path(get_val(<<"keyfile">>, Raw, undefined)),
        server_name_indication => parse_sni(
            get_val(<<"sni">>, Raw, undefined)
        )
    };
parse_ssl(_) ->
    #{
        enable => false,
        verify => verify_none,
        cacertfile => undefined,
        certfile => undefined,
        keyfile => undefined,
        server_name_indication => undefined
    }.

parse_sni(undefined) -> undefined;
parse_sni(<<"disable">>) -> disable;
parse_sni(V) when is_binary(V) -> binary_to_list(V);
parse_sni(V) when is_list(V) -> V;
parse_sni(_) -> undefined.

parse_file_path(undefined) -> undefined;
parse_file_path(<<>>) -> undefined;
parse_file_path(V) when is_binary(V) -> binary_to_list(V);
parse_file_path(V) when is_list(V) -> V;
parse_file_path(_) -> undefined.

parse_verify(<<"verify_peer">>) -> verify_peer;
parse_verify(_) -> verify_none.

parse_clientid_prefix(<<>>, Name) ->
    <<"emqx-dq-", Name/binary, "-">>;
parse_clientid_prefix(Val, _Name) ->
    to_bin(Val).

parse_queue_base_dir(#{} = QueueConf) ->
    resolve_data_dir(to_bin(get_val(<<"base_dir">>, QueueConf, <<>>)));
parse_queue_base_dir(_) ->
    resolve_data_dir(<<>>).

resolve_data_dir(<<"/", _/binary>> = AbsPath) ->
    AbsPath;
resolve_data_dir(<<>>) ->
    %% Default base dir when not specified
    resolve_data_dir(<<"emqx_bridge_mqtt_dq">>);
resolve_data_dir(RelPath) ->
    DataDir = emqx:data_dir(),
    to_bin(filename:join(DataDir, binary_to_list(RelPath))).

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

format_reason(Bin) when is_binary(Bin) -> Bin;
format_reason(Str) when is_list(Str) -> list_to_binary(Str);
format_reason(Term) -> fmt("~tp", [Term]).

fmt(Fmt, Args) ->
    iolist_to_binary(io_lib:format(Fmt, Args)).
