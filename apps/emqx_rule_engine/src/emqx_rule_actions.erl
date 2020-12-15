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

%% Define the default actions.
-module(emqx_rule_actions).

-include("rule_engine.hrl").
-include("rule_actions.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(REPUBLISH_PARAMS_SPEC, #{
            target_topic => #{
                order => 1,
                type => string,
                required => true,
                default => <<"repub/to/${clientid}">>,
                title => #{en => <<"Target Topic">>,
                           zh => <<"目的主题"/utf8>>},
                description => #{en => <<"To which topic the message will be republished">>,
                                 zh => <<"重新发布消息到哪个主题"/utf8>>}
            },
            target_qos => #{
                order => 2,
                type => number,
                enum => [-1, 0, 1, 2],
                required => true,
                default => 0,
                title => #{en => <<"Target QoS">>,
                           zh => <<"目的 QoS"/utf8>>},
                description => #{en => <<"The QoS Level to be uses when republishing the message. Set to -1 to use the original QoS">>,
                                 zh => <<"重新发布消息时用的 QoS 级别, 设置为 -1 以使用原消息中的 QoS"/utf8>>}
            },
            payload_tmpl => #{
                order => 3,
                type => string,
                input => textarea,
                required => false,
                default => <<"${payload}">>,
                title => #{en => <<"Payload Template">>,
                           zh => <<"消息内容模板"/utf8>>},
                description => #{en => <<"The payload template, variable interpolation is supported">>,
                                 zh => <<"消息内容模板，支持变量"/utf8>>}
            }
        }).

-rule_action(#{name => inspect,
               category => debug,
               for => '$any',
               types => [],
               create => on_action_create_inspect,
               params => #{},
               title => #{en => <<"Inspect (debug)">>,
                          zh => <<"检查 (调试)"/utf8>>},
               description => #{en => <<"Inspect the details of action params for debug purpose">>,
                                zh => <<"检查动作参数 (用以调试)"/utf8>>}
              }).

-rule_action(#{name => republish,
               category => data_forward,
               for => '$any',
               types => [],
               create => on_action_create_republish,
               params => ?REPUBLISH_PARAMS_SPEC,
               title => #{en => <<"Republish">>,
                          zh => <<"消息重新发布"/utf8>>},
               description => #{en => <<"Republish a MQTT message to another topic">>,
                                zh => <<"重新发布消息到另一个主题"/utf8>>}
              }).

-rule_action(#{name => do_nothing,
               category => debug,
               for => '$any',
               types => [],
               create => on_action_create_do_nothing,
               params => #{},
               title => #{en => <<"Do Nothing (debug)">>,
                          zh => <<"空动作 (调试)"/utf8>>},
               description => #{en => <<"This action does nothing and never fails. It's for debug purpose">>,
                                zh => <<"此动作什么都不做，并且不会失败 (用以调试)"/utf8>>}
              }).

-export([on_resource_create/2]).

%% callbacks for rule engine
-export([ on_action_create_inspect/2
        , on_action_create_republish/2
        , on_action_create_do_nothing/2
        ]).

-export([ on_action_inspect/2
        , on_action_republish/2
        , on_action_do_nothing/2
        ]).

-spec(on_resource_create(binary(), map()) -> map()).
on_resource_create(_Name, Conf) ->
    Conf.

%%------------------------------------------------------------------------------
%% Action 'inspect'
%%------------------------------------------------------------------------------
-spec on_action_create_inspect(action_instance_id(), Params :: map())
    -> {bindings(), NewParams :: map()}.
on_action_create_inspect(Id, Params) ->
    Params.

-spec on_action_inspect(selected_data(), env_vars()) -> any().
on_action_inspect(Selected, Envs) ->
    io:format("[inspect]~n"
              "\tSelected Data: ~p~n"
              "\tEnvs: ~p~n"
              "\tAction Init Params: ~p~n", [Selected, Envs, ?bound_v('Params', Envs)]),
    emqx_rule_metrics:inc_actions_success(?bound_v('Id', Envs)).


%%------------------------------------------------------------------------------
%% Action 'republish'
%%------------------------------------------------------------------------------
-spec on_action_create_republish(action_instance_id(), Params :: map())
      -> {bindings(), NewParams :: map()}.
on_action_create_republish(Id, Params = #{
        <<"target_topic">> := TargetTopic,
        <<"target_qos">> := TargetQoS,
        <<"payload_tmpl">> := PayloadTmpl
       }) ->
    TopicTks = emqx_rule_utils:preproc_tmpl(TargetTopic),
    PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
    Params.

-spec on_action_republish(selected_data(), env_vars()) -> any().
on_action_republish(_Selected, Envs = #{
            topic := Topic,
            headers := #{republish_by := ActId},
            ?BINDING_KEYS := #{'Id' := ActId}
        }) ->
    ?LOG(error, "[republish] recursively republish detected, msg topic: ~p, target topic: ~p",
        [Topic, ?bound_v('TargetTopic', Envs)]),
    emqx_rule_metrics:inc_actions_error(?bound_v('Id', Envs));

on_action_republish(Selected, _Envs = #{
            qos := QoS, flags := Flags, timestamp := Timestamp,
            ?BINDING_KEYS := #{
                'Id' := ActId,
                'TargetTopic' := TargetTopic,
                'TargetQoS' := TargetQoS,
                'TopicTks' := TopicTks,
                'PayloadTks' := PayloadTks
            }}) ->
    ?LOG(debug, "[republish] republish to: ~p, Payload: ~p",
        [TargetTopic, Selected]),
    increase_and_publish(ActId,
        #message{
            id = emqx_guid:gen(),
            qos = if TargetQoS =:= -1 -> QoS; true -> TargetQoS end,
            from = ActId,
            flags = Flags,
            headers = #{republish_by => ActId},
            topic = emqx_rule_utils:proc_tmpl(TopicTks, Selected),
            payload = emqx_rule_utils:proc_tmpl(PayloadTks, Selected),
            timestamp = Timestamp
        });

%% in case this is not a "message.publish" request
on_action_republish(Selected, _Envs = #{
            ?BINDING_KEYS := #{
                'Id' := ActId,
                'TargetTopic' := TargetTopic,
                'TargetQoS' := TargetQoS,
                'TopicTks' := TopicTks,
                'PayloadTks' := PayloadTks
            }}) ->
    ?LOG(debug, "[republish] republish to: ~p, Payload: ~p",
        [TargetTopic, Selected]),
    increase_and_publish(ActId,
        #message{
            id = emqx_guid:gen(),
            qos = if TargetQoS =:= -1 -> 0; true -> TargetQoS end,
            from = ActId,
            flags = #{dup => false, retain => false},
            headers = #{republish_by => ActId},
            topic = emqx_rule_utils:proc_tmpl(TopicTks, Selected),
            payload = emqx_rule_utils:proc_tmpl(PayloadTks, Selected),
            timestamp = erlang:system_time(millisecond)
        }).

increase_and_publish(ActId, Msg) ->
    _ = emqx_broker:safe_publish(Msg),
    emqx_rule_metrics:inc_actions_success(ActId),
    emqx_metrics:inc_msg(Msg).

-spec on_action_create_do_nothing(action_instance_id(), Params :: map())
      -> {bindings(), NewParams :: map()}.
on_action_create_do_nothing(ActId, Params) when is_binary(ActId) ->
    Params.

on_action_do_nothing(Selected, Envs) when is_map(Selected) ->
    emqx_rule_metrics:inc_actions_success(?bound_v('ActId', Envs)).
