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

-module(emqx_mqtt_props_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_ct_helpers/include/emqx_ct.hrl").

all() -> emqx_ct:all(?MODULE).

t_id(_) ->
    foreach_prop(
      fun({Id, Prop}) ->
              ?assertEqual(Id, emqx_mqtt_props:id(element(1, Prop)))
      end),
    ?catch_error({bad_property, 'Bad-Property'}, emqx_mqtt_props:id('Bad-Property')).

t_name(_) ->
    foreach_prop(
      fun({Id, Prop}) ->
              ?assertEqual(emqx_mqtt_props:name(Id), element(1, Prop))
      end),
    ?catch_error({unsupported_property, 16#FF}, emqx_mqtt_props:name(16#FF)).

t_filter(_) ->
    ConnProps = #{'Session-Expiry-Interval' => 1,
                  'Maximum-Packet-Size' => 255
                 },
    ?assertEqual(ConnProps,
                 emqx_mqtt_props:filter(?CONNECT, ConnProps)),
    PubProps = #{'Payload-Format-Indicator' => 6,
                 'Message-Expiry-Interval' => 300,
                 'Session-Expiry-Interval' => 300
                },
    ?assertEqual(#{'Payload-Format-Indicator' => 6,
                   'Message-Expiry-Interval' => 300
                  },
                 emqx_mqtt_props:filter(?PUBLISH, PubProps)).

t_validate(_) ->
    ConnProps = #{'Session-Expiry-Interval' => 1,
                  'Maximum-Packet-Size' => 255
                 },
    ok = emqx_mqtt_props:validate(ConnProps),
    BadProps = #{'Unknown-Property' => 10},
    ?catch_error({bad_property,'Unknown-Property'},
                 emqx_mqtt_props:validate(BadProps)).

t_validate_value(_) ->
    ok = emqx_mqtt_props:validate(#{'Correlation-Data' => <<"correlation-id">>}),
    ok = emqx_mqtt_props:validate(#{'Reason-String' => <<"Unknown Reason">>}),
    ok = emqx_mqtt_props:validate(#{'User-Property' => {<<"Prop">>, <<"Val">>}}),
    ok = emqx_mqtt_props:validate(#{'User-Property' => [{<<"Prop">>, <<"Val">>}]}),
    ?catch_error({bad_property_value, {'Payload-Format-Indicator', 16#FFFF}},
                 emqx_mqtt_props:validate(#{'Payload-Format-Indicator' => 16#FFFF})),
    ?catch_error({bad_property_value, {'Server-Keep-Alive', 16#FFFFFF}},
                 emqx_mqtt_props:validate(#{'Server-Keep-Alive' => 16#FFFFFF})),
    ?catch_error({bad_property_value, {'Will-Delay-Interval', -16#FF}},
                 emqx_mqtt_props:validate(#{'Will-Delay-Interval' => -16#FF})).

foreach_prop(Fun) ->
    lists:foreach(Fun, maps:to_list(emqx_mqtt_props:all())).


% t_all(_) ->
%     error('TODO').

% t_set(_) ->
%     error('TODO').

% t_get(_) ->
%     error('TODO').