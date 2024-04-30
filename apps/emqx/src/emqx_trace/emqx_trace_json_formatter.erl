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
    %% We just make some basic transformations on the input LogMap and then do
    %% an external call to create the JSON text
    Time = emqx_utils_calendar:now_to_rfc3339(microsecond),
    LogMap1 = LogMap#{time => Time},
    LogMap2 = prepare_log_map(LogMap1, PEncode),
    [emqx_logger_jsonfmt:best_effort_json(LogMap2, [force_utf8]), "\n"].

%%%-----------------------------------------------------------------
%%% Helper Functions
%%%-----------------------------------------------------------------

prepare_log_map(LogMap, PEncode) ->
    NewKeyValuePairs = [prepare_key_value(K, V, PEncode) || {K, V} <- maps:to_list(LogMap)],
    maps:from_list(NewKeyValuePairs).

prepare_key_value(K, {Formatter, V}, PEncode) when is_function(Formatter, 1) ->
    %% A cusom formatter is provided with the value
    try
        NewV = Formatter(V),
        prepare_key_value(K, NewV, PEncode)
    catch
        _:_ ->
            {K, V}
    end;
prepare_key_value(K, {ok, {Formatter, V}}, PEncode) when is_function(Formatter, 1) ->
    %% Unwrap
    prepare_key_value(K, {Formatter, V}, PEncode);
prepare_key_value(host, {I1, I2, I3, I4}, _PEncode) when
    is_integer(I1),
    is_integer(I2),
    is_integer(I3),
    is_integer(I4)
->
    %% We assume this is an IP address
    {host,
        unicode:characters_to_binary([
            integer_to_binary(I1),
            <<".">>,
            integer_to_binary(I2),
            <<".">>,
            integer_to_binary(I3),
            <<".">>,
            integer_to_binary(I4)
        ])};
prepare_key_value(K, {ok, StatusCode, Headers}, PEncode) when
    is_integer(StatusCode), StatusCode >= 200, StatusCode < 300, is_list(Headers)
->
    prepare_key_value(K, {ok, StatusCode, Headers, <<"">>}, PEncode);
prepare_key_value(K, {ok, StatusCode, Headers, Body}, PEncode) when
    is_integer(StatusCode), StatusCode >= 200, StatusCode < 300, is_list(Headers)
->
    %% We assume this is that response of an HTTP request
    prepare_key_value(
        K,
        #{
            result => ok,
            response => #{
                status => StatusCode,
                headers => Headers,
                body => Body
            }
        },
        PEncode
    );
prepare_key_value(payload = K, V, PEncode) ->
    NewV =
        try
            format_payload(V, PEncode)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(packet = K, V, PEncode) ->
    NewV =
        try
            format_packet(V, PEncode)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(rule_ids = K, V, _PEncode) ->
    NewV =
        try
            format_map_set_to_list(V)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(client_ids = K, V, _PEncode) ->
    NewV =
        try
            format_map_set_to_list(V)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(action_id = K, V, _PEncode) ->
    try
        {action_info, format_action_info(V)}
    catch
        _:_ ->
            {K, V}
    end;
prepare_key_value(K, V, PEncode) when is_map(V) ->
    {K, prepare_log_map(V, PEncode)};
prepare_key_value(K, V, _PEncode) ->
    {K, V}.

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

format_map_set_to_list(Map) ->
    Items = [
        begin
            %% Assert that it is really a map set
            true = V,
            %% Assert that the keys have the expected type
            true = is_binary(K),
            K
        end
     || {K, V} <- maps:to_list(Map)
    ],
    lists:sort(Items).

format_action_info(#{mod := _Mod, func := _Func} = FuncCall) ->
    FuncCall;
format_action_info(V) ->
    [<<"action">>, Type, Name | _] = binary:split(V, <<":">>, [global]),
    #{
        type => Type,
        name => Name
    }.
