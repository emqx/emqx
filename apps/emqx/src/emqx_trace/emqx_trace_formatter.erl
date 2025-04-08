%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_trace_formatter).
-include("emqx_mqtt.hrl").
-include("emqx_trace.hrl").

-export([format/2]).

%% logger_formatter:config/0 is not exported.
-type config() :: map().

%%%-----------------------------------------------------------------
%%% API
-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: config().
format(
    #{level := debug, meta := _Meta = #{trace_tag := _Tag}, msg := _Msg} = Entry,
    #{payload_fmt_opts := PayloadFmtOpts}
) ->
    #{level := debug, meta := Meta = #{trace_tag := Tag}, msg := Msg} =
        emqx_logger_textfmt:evaluate_lazy_values(Entry),
    Time = emqx_utils_calendar:now_to_rfc3339(microsecond),
    Tns =
        case to_iolist(maps:get(tns, Meta, "")) of
            "" -> "";
            X -> [" tns: ", X]
        end,
    ClientId = to_iolist(maps:get(clientid, Meta, "")),
    Peername = maps:get(peername, Meta, ""),
    MetaBin = format_meta(Meta, PayloadFmtOpts),
    Msg1 = to_iolist(Msg),
    Tag1 = to_iolist(Tag),
    [Time, " [", Tag1, "] ", ClientId, "@", Peername, Tns, " msg: ", Msg1, ", ", MetaBin, "\n"];
format(Event, Config) ->
    emqx_logger_textfmt:format(Event, Config).

format_meta_map(Meta, PayloadFmtOpts) ->
    format_meta_map(Meta, PayloadFmtOpts, [
        {?FORMAT_META_KEY_PACKET, fun format_packet/2},
        {?FORMAT_META_KEY_PAYLOAD, fun format_payload/2},
        {?FORMAT_META_KEY_PAYLOAD_BIN, fun format_payload/2},
        {?FORMAT_META_KEY_RESULT, fun format_result/2}
    ]).

format_meta_map(Meta, _PayloadFmtOpts, []) ->
    Meta;
format_meta_map(Meta, PayloadFmtOpts, [{Name, FormatFun} | Rest]) ->
    case Meta of
        #{Name := Value} ->
            NewMeta =
                case FormatFun(Value, PayloadFmtOpts) of
                    {NewValue, NewMeta0} ->
                        maps:merge(Meta#{Name => NewValue}, NewMeta0);
                    NewValue ->
                        Meta#{Name => NewValue}
                end,
            format_meta_map(NewMeta, PayloadFmtOpts, Rest);
        #{} ->
            format_meta_map(Meta, PayloadFmtOpts, Rest)
    end.

format_meta_data(Meta0, PayloadFmtOpts) when is_map(Meta0) ->
    Meta1 = format_meta_map(Meta0, PayloadFmtOpts),
    maps:map(fun(_K, V) -> format_meta_data(V, PayloadFmtOpts) end, Meta1);
format_meta_data(Meta, PayloadFmtOpts) when is_list(Meta) ->
    [format_meta_data(Item, PayloadFmtOpts) || Item <- Meta];
format_meta_data(Meta, PayloadFmtOpts) when is_tuple(Meta) ->
    List = erlang:tuple_to_list(Meta),
    FormattedList = [format_meta_data(Item, PayloadFmtOpts) || Item <- List],
    erlang:list_to_tuple(FormattedList);
format_meta_data(Meta, _PayloadFmtOpts) ->
    Meta.

format_meta(Meta0, PayloadFmtOpts) ->
    Meta1 = maps:without([msg, tns, clientid, peername, trace_tag], Meta0),
    Meta2 = format_meta_data(Meta1, PayloadFmtOpts),
    kvs_to_iolist(lists:sort(fun compare_meta_kvs/2, maps:to_list(Meta2))).

%% packet always goes first; payload always goes last
compare_meta_kvs(KV1, KV2) -> weight(KV1) =< weight(KV2).

weight({packet, _}) -> {0, packet};
weight({payload, _}) -> {2, payload};
weight({K, _}) -> {1, K}.

format_packet(undefined, _) ->
    "";
format_packet(Packet, PayloadFmtOpts) ->
    try
        emqx_packet:format(Packet, PayloadFmtOpts)
    catch
        _:_ ->
            %% We don't want to crash if there is a field named packet with
            %% some other type of value
            Packet
    end.

format_payload(undefined, #{payload_encode := Type}) ->
    {"", #{payload_encode => Type}};
format_payload(Payload, PayloadFmtOpts) when is_binary(Payload) ->
    {Payload1, Type1} = emqx_packet:format_payload(Payload, PayloadFmtOpts),
    {Payload1, #{payload_encode => Type1}};
format_payload(Payload, #{payload_encode := Type}) ->
    {Payload, #{payload_encode => Type}}.

format_result(undefined, #{payload_encode := Type}) ->
    {"", #{payload_encode => Type}};
format_result(Result0, PayloadFmtOpts) ->
    Result = iolist_to_binary(to_iolist(Result0)),
    Type = maps:get(payload_encode, PayloadFmtOpts),
    Limit = maps:get(truncate_above, PayloadFmtOpts, ?MAX_PAYLOAD_FORMAT_SIZE),
    TruncateTo = maps:get(truncate_to, PayloadFmtOpts, ?TRUNCATED_PAYLOAD_SIZE),
    format_truncated_result(Type, Limit, TruncateTo, Result).

format_truncated_result(Type, Limit, TruncateTo, Result) when
    size(Result) > Limit
->
    {NType, Part, TruncatedBytes} = truncate_result(Type, TruncateTo, Result),
    {?TRUNCATED_IOLIST(do_format_result(NType, Part), TruncatedBytes), #{payload_encode => NType}};
format_truncated_result(Type, _Limit, _TruncateTo, Result) ->
    %% truncate to itself size
    {NType, _Part, _TruncatedBytes} = truncate_result(Type, size(Result), Result),
    {do_format_result(NType, Result), #{payload_encode => NType}}.

truncate_result(hex, Limit, Result) ->
    <<Part:Limit/binary, Rest/binary>> = Result,
    {hex, Part, size(Rest)};
truncate_result(text, Limit, Result) ->
    case emqx_utils_fmt:find_complete_utf8_len(Limit, Result) of
        {ok, Len} ->
            <<Part:Len/binary, Rest/binary>> = Result,
            {text, Part, size(Rest)};
        error ->
            %% not a valid utf8 string, force hex
            <<Part:Limit/binary, Rest/binary>> = Result,
            {hex, Part, size(Rest)}
    end.

do_format_result(text, Bytes) ->
    %% utf8 ensured
    Bytes;
do_format_result(hex, Bytes) ->
    binary:encode_hex(Bytes).

to_iolist(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_iolist(Int) when is_integer(Int) -> integer_to_list(Int);
to_iolist(Float) when is_float(Float) -> float_to_list(Float, [{decimals, 2}]);
to_iolist(SubMap) when is_map(SubMap) -> ["[", kvs_to_iolist(maps:to_list(SubMap)), "]"];
to_iolist(Char) -> emqx_logger_textfmt:try_format_unicode(Char).

kvs_to_iolist(KVs) ->
    lists:join(
        ", ",
        lists:map(
            fun({K, V}) -> [to_iolist(K), ": ", to_iolist(V)] end,
            KVs
        )
    ).
