%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_trace_json_formatter).

-include("emqx_mqtt.hrl").

-export([format/2]).

%% logger_formatter:config/0 is not exported.
-type config() :: map().

%%%-----------------------------------------------------------------
%%% Callback Function
%%%-----------------------------------------------------------------

-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: config().
format(
    LogMap,
    #{payload_encode := PEncode}
) ->
    Time = emqx_utils_calendar:now_to_rfc3339(microsecond),
    LogMap1 = LogMap#{time => Time},
    [format_log_map(LogMap1, PEncode), "\n"].

%%%-----------------------------------------------------------------
%%% Helper Functions
%%%-----------------------------------------------------------------

format_log_map(Map, PEncode) ->
    KeyValuePairs = format_key_value_pairs(maps:to_list(Map), PEncode, []),
    ["{", KeyValuePairs, "}"].

format_key_value_pairs([], _PEncode, Acc) ->
    lists:join(",", Acc);
format_key_value_pairs([{payload, Value} | Rest], PEncode, Acc) ->
    FormattedPayload = format_payload(Value, PEncode),
    FormattedPayloadEscaped = escape(FormattedPayload),
    Pair = ["\"payload\": \"", FormattedPayloadEscaped, "\""],
    format_key_value_pairs(Rest, PEncode, [Pair | Acc]);
format_key_value_pairs([{packet, Value} | Rest], PEncode, Acc) ->
    Formatted = format_packet(Value, PEncode),
    FormattedEscaped = escape(Formatted),
    Pair = ["\"packet\": \"", FormattedEscaped, "\""],
    format_key_value_pairs(Rest, PEncode, [Pair | Acc]);
format_key_value_pairs([{Key, Value} | Rest], PEncode, Acc) ->
    FormattedKey = format_key(Key),
    FormattedValue = format_value(Value, PEncode),
    Pair = ["\"", FormattedKey, "\":", FormattedValue],
    format_key_value_pairs(Rest, PEncode, [Pair | Acc]).

format_key(Term) ->
    %% Keys must be strings
    String = emqx_logger_textfmt:try_format_unicode(Term),
    escape(String).

format_value(Map, PEncode) when is_map(Map) ->
    format_log_map(Map, PEncode);
format_value(V, _PEncode) when is_integer(V) ->
    integer_to_list(V);
format_value(V, _PEncode) when is_float(V) ->
    float_to_list(V, [{decimals, 2}]);
format_value(true, _PEncode) ->
    "true";
format_value(false, _PEncode) ->
    "false";
format_value(V, _PEncode) ->
    String = emqx_logger_textfmt:try_format_unicode(V),
    ["\"", escape(String), "\""].

escape(IOList) ->
    Bin = iolist_to_binary(IOList),
    List = binary_to_list(Bin),
    escape_list(List).

escape_list([]) ->
    [];
escape_list([$\n | Rest]) ->
    %% 92 is backslash
    [92, $n | escape_list(Rest)];
escape_list([$" | Rest]) ->
    [92, $" | escape_list(Rest)];
escape_list([92 | Rest]) ->
    [92, 92 | escape_list(Rest)];
escape_list([X | Rest]) ->
    [X | escape_list(Rest)].

format_packet(undefined, _) -> "";
format_packet(Packet, Encode) -> emqx_packet:format(Packet, Encode).

format_payload(undefined, _) ->
    "";
format_payload(_, hidden) ->
    "******";
format_payload(Payload, text) when ?MAX_PAYLOAD_FORMAT_LIMIT(Payload) ->
    unicode:characters_to_list(Payload);
format_payload(Payload, hex) when ?MAX_PAYLOAD_FORMAT_LIMIT(Payload) -> binary:encode_hex(Payload);
format_payload(<<Part:?TRUNCATED_PAYLOAD_SIZE/binary, _/binary>> = Payload, Type) ->
    emqx_packet:format_truncated_payload(Part, byte_size(Payload), Type).
