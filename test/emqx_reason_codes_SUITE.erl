%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_reason_codes_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_frame_error(_) ->
    ?assertEqual(?RC_PACKET_TOO_LARGE, emqx_reason_codes:frame_error(frame_too_large)),
    ?assertEqual(?RC_MALFORMED_PACKET, emqx_reason_codes:frame_error(bad_packet_id)),
    ?assertEqual(?RC_MALFORMED_PACKET, emqx_reason_codes:frame_error(bad_qos)).

t_prop_name_text(_) ->
    ?assert(proper:quickcheck(prop_name_text(), prop_name_text(opts))).

t_prop_compat(_) ->
    ?assert(proper:quickcheck(prop_compat(), prop_compat(opts))).

t_prop_connack_error(_) ->
    ?assert(proper:quickcheck(prop_connack_error(), default_opts([]))).

prop_name_text(opts) ->
    default_opts([{numtests, 1000}]).

prop_name_text() ->
    ?FORALL(UnionArgs, union_args(),
            is_atom(apply_fun(name, UnionArgs)) andalso
            is_binary(apply_fun(text, UnionArgs))).

prop_compat(opts) ->
    default_opts([{numtests, 512}]).

prop_compat() ->
    ?FORALL(CompatArgs, compat_args(),
            begin
                Result = apply_fun(compat, CompatArgs),
                is_number(Result) orelse Result =:= undefined
            end).

prop_connack_error() ->
    ?FORALL(CONNACK_ERROR_ARGS, connack_error_args(),
            is_integer(apply_fun(connack_error, CONNACK_ERROR_ARGS))).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

default_opts() ->
    default_opts([]).

default_opts(AdditionalOpts) ->
    [{to_file, user} | AdditionalOpts].

apply_fun(Fun, Args) ->
    apply(emqx_reason_codes, Fun, Args).

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------

union_args() ->
    frequency([{6, [real_mqttv3_rc(), mqttv3_version()]},
               {43, [real_mqttv5_rc(), mqttv5_version()]}]).

compat_args() ->
    frequency([{18, [connack, compat_rc()]},
               {2, [suback, compat_rc()]},
               {1, [unsuback, compat_rc()]}]).

connack_error_args() ->
    [frequency([{10, connack_error()},
                {1, unexpected_connack_error()}])].

connack_error() ->
    oneof([client_identifier_not_valid,
           bad_username_or_password,
           bad_clientid_or_password,
           username_or_password_undefined,
           password_error,
           not_authorized,
           server_unavailable,
           server_busy,
           banned,
           bad_authentication_method]).

unexpected_connack_error() ->
    oneof([who_knows]).


real_mqttv3_rc() ->
    frequency([{6, mqttv3_rc()},
               {1, unexpected_rc()}]).

real_mqttv5_rc() ->
    frequency([{43, mqttv5_rc()},
               {2, unexpected_rc()}]).

compat_rc() ->
    frequency([{95, ?SUCHTHAT(RC , mqttv5_rc(), RC >= 16#80 orelse RC =< 2)},
               {5, unexpected_rc()}]).

mqttv3_rc() ->
    oneof(mqttv3_rcs()).

mqttv5_rc() ->
    oneof(mqttv5_rcs()).

unexpected_rc() ->
    oneof(unexpected_rcs()).

mqttv3_rcs() ->
    [0, 1, 2, 3, 4, 5].

mqttv5_rcs() ->
    [16#00, 16#01, 16#02, 16#04, 16#10, 16#11, 16#18, 16#19,
     16#80, 16#81, 16#82, 16#83, 16#84, 16#85, 16#86, 16#87,
     16#88, 16#89, 16#8A, 16#8B, 16#8C, 16#8D, 16#8E, 16#8F,
     16#90, 16#91, 16#92, 16#93, 16#94, 16#95, 16#96, 16#97,
     16#98, 16#99, 16#9A, 16#9B, 16#9C, 16#9D, 16#9E, 16#9F,
     16#A0, 16#A1, 16#A2].

unexpected_rcs() ->
    ReasonCodes = mqttv3_rcs() ++ mqttv5_rcs(),
    Unexpected = lists:seq(0, 16#FF) -- ReasonCodes,
    lists:sublist(Unexpected, 5).

mqttv5_version() ->
    ?MQTT_PROTO_V5.

mqttv3_version() ->
    oneof([?MQTT_PROTO_V3, ?MQTT_PROTO_V4]).

