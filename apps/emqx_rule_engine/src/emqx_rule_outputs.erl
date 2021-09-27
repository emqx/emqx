%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Define the default actions.
-module(emqx_rule_outputs).
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").

-export([ console/3
        , republish/3
        ]).

-spec console(map(), map(), map()) -> any().
console(Selected, #{metadata := #{rule_id := RuleId}} = Envs, _Args) ->
    ?ULOG("[rule output] ~s~n"
          "\tOutput Data: ~p~n"
          "\tEnvs: ~p~n", [RuleId, Selected, Envs]).

republish(_Selected, #{topic := Topic, headers := #{republish_by := RuleId},
        metadata := #{rule_id := RuleId}}, _Args) ->
    ?SLOG(error, #{msg => "[republish] recursively republish detected", topic => Topic});

%% republish a PUBLISH message
republish(Selected, #{flags := Flags, metadata := #{rule_id := RuleId}},
        #{preprocessed_tmpl := #{
            qos := QoSTks,
            retain := RetainTks,
            topic := TopicTks,
            payload := PayloadTks}}) ->
    Topic = emqx_plugin_libs_rule:proc_tmpl(TopicTks, Selected),
    Payload = emqx_plugin_libs_rule:proc_tmpl(PayloadTks, Selected),
    QoS = replace_simple_var(QoSTks, Selected),
    Retain = replace_simple_var(RetainTks, Selected),
    ?SLOG(debug, #{msg => "republish", topic => Topic, payload => Payload}),
    safe_publish(RuleId, Topic, QoS, Flags#{retain => Retain}, Payload);

%% in case this is a "$events/" event
republish(Selected, #{metadata := #{rule_id := RuleId}},
        #{preprocessed_tmpl := #{
                qos := QoSTks,
                retain := RetainTks,
                topic := TopicTks,
                payload := PayloadTks}}) ->
    Topic = emqx_plugin_libs_rule:proc_tmpl(TopicTks, Selected),
    Payload = emqx_plugin_libs_rule:proc_tmpl(PayloadTks, Selected),
    QoS = replace_simple_var(QoSTks, Selected),
    Retain = replace_simple_var(RetainTks, Selected),
    ?SLOG(debug, #{msg => "republish", topic => Topic, payload => Payload}),
    safe_publish(RuleId, Topic, QoS, #{retain => Retain}, Payload).

safe_publish(RuleId, Topic, QoS, Flags, Payload) ->
    Msg = #message{
        id = emqx_guid:gen(),
        qos = QoS,
        from = RuleId,
        flags = Flags,
        headers = #{republish_by => RuleId},
        topic = Topic,
        payload = Payload,
        timestamp = erlang:system_time(millisecond)
    },
    _ = emqx_broker:safe_publish(Msg),
    emqx_metrics:inc_msg(Msg).

replace_simple_var(Tokens, Data) when is_list(Tokens) ->
    [Var] = emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Var;
replace_simple_var(Val, _Data) ->
    Val.