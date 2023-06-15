%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(BAD_TOPIC_WITH_WILDCARD, wildcard_topic_not_allowed_for_publish).

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
                input => editable_select,
                type => [number, string],
                enum => [0, 1, 2, <<"${qos}">>],
                required => true,
                default => 0,
                title => #{en => <<"Target QoS">>,
                           zh => <<"目的 QoS"/utf8>>},
                description => #{en =>
                                    <<"The QoS Level to be used when republishing the message."
                                      " Support placeholder variables."
                                      " Set to ${qos} to use the original QoS. Default is 0">>,
                                 zh =>
                                    <<"重新发布消息时用的 QoS 级别。"
                                      "支持占位符变量，可以填写 ${qos} 来使用原消息的 QoS。默认 0"/utf8>>}
            },
            target_retain => #{
                order => 3,
                input => editable_select,
                type => [boolean, string],
                enum => [true, false, <<"${flags.retain}">>],
                required => false,
                default => false,
                title => #{en => <<"Target Retain">>,
                           zh => <<"目标保留消息标识"/utf8>>},
                description => #{en => <<"The Retain flag to be used when republishing the message."
                                         " Set to ${flags.retain} to use the original Retain."
                                         " Support placeholder variables. Default is false">>,
                                 zh => <<"重新发布消息时用的保留消息标识。"
                                         "支持占位符变量，可以填写 ${flags.retain} 来使用原消息的 Retain。"
                                         "默认 false"/utf8>>}
            },
            payload_tmpl => #{
                order => 4,
                type => string,
                input => textarea,
                required => false,
                default => <<"${payload}">>,
                title => #{en => <<"Payload Template">>,
                           zh => <<"消息内容模板"/utf8>>},
                description => #{en => <<"The payload template, "
                                         "variable interpolation is supported">>,
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
               description => #{en => <<"This action does nothing and never fails. "
                                        "It's for debug purpose">>,
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
-spec on_action_create_inspect(Id :: action_instance_id(), Params :: map()) ->
    {bindings(), NewParams :: map()}.
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
-spec on_action_create_republish(action_instance_id(), Params :: map()) ->
    {bindings(), NewParams :: map()}.
on_action_create_republish(Id, Params = #{
        <<"target_topic">> := TargetTopic,
        <<"target_qos">> := TargetQoS0,
        <<"payload_tmpl">> := PayloadTmpl
       }) ->
    TargetRetain = to_retain(maps:get(<<"target_retain">>, Params, <<"false">>)),
    TargetQoS = to_qos(TargetQoS0),
    TopicTks = emqx_rule_utils:preproc_tmpl(assert_topic_valid(TargetTopic)),
    PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
    Params.

-spec on_action_republish(selected_data(), env_vars()) -> any().
on_action_republish(_Selected, Envs = #{
            topic := Topic,
            headers := #{republish_by := ActId},
            ?BINDING_KEYS := #{'Id' := ActId},
            metadata := Metadata
        }) ->
    ?LOG_RULE_ACTION(
        error,
        Metadata,
        "[republish] recursively republish detected, msg topic: ~p, target topic: ~p",
        [Topic, ?bound_v('TargetTopic', Envs)]),
    emqx_rule_metrics:inc_actions_error(?bound_v('Id', Envs)),
    {badact, recursively_republish};

on_action_republish(Selected, _Envs = #{
            qos := QoS, flags := Flags, timestamp := Timestamp,
            ?BINDING_KEYS := #{
                'Id' := ActId,
                'TargetTopic' := TargetTopic,
                'TargetQoS' := TargetQoS,
                'TopicTks' := TopicTks,
                'PayloadTks' := PayloadTks
            } = Bindings,
            metadata := Metadata}) ->
    ?LOG_RULE_ACTION(debug, Metadata, "[republish] republish to: ~p, Selected: ~p", [TargetTopic, Selected]),
    TargetRetain = maps:get('TargetRetain', Bindings, false),
    Message =
        #message{
            id = emqx_guid:gen(),
            qos = get_qos(TargetQoS, Selected, QoS),
            from = ActId,
            flags = Flags#{retain => get_retain(TargetRetain, Selected)},
            headers = #{republish_by => ActId},
            topic = assert_topic_valid(emqx_rule_utils:proc_tmpl(TopicTks, Selected)),
            payload = format_msg(PayloadTks, Selected),
            timestamp = Timestamp
        },
    increase_and_publish(ActId, Message);

%% in case this is not a "message.publish" request
on_action_republish(Selected, _Envs = #{
            ?BINDING_KEYS := #{
                'Id' := ActId,
                'TargetTopic' := TargetTopic,
                'TargetQoS' := TargetQoS,
                'TopicTks' := TopicTks,
                'PayloadTks' := PayloadTks
            } = Bindings,
            metadata := Metadata}) ->
    ?LOG_RULE_ACTION(debug, Metadata, "[republish] republish to: ~p, Selected: ~p", [TargetTopic, Selected]),
    TargetRetain = maps:get('TargetRetain', Bindings, false),
    Message =
        #message{
            id = emqx_guid:gen(),
            qos = get_qos(TargetQoS, Selected, 0),
            from = ActId,
            flags = #{dup => false, retain => get_retain(TargetRetain, Selected)},
            headers = #{republish_by => ActId},
            topic = assert_topic_valid(emqx_rule_utils:proc_tmpl(TopicTks, Selected)),
            payload = format_msg(PayloadTks, Selected),
            timestamp = erlang:system_time(millisecond)
        },
    increase_and_publish(ActId, Message).

increase_and_publish(ActId, Msg) ->
    _ = emqx_broker:safe_publish(Msg),
    emqx_rule_metrics:inc_actions_success(ActId),
    emqx_metrics:inc_msg(Msg).

-spec on_action_create_do_nothing(action_instance_id(), Params :: map()) ->
    {bindings(), NewParams :: map()}.
on_action_create_do_nothing(ActId, Params) when is_binary(ActId) ->
    Params.

on_action_do_nothing(Selected, Envs) when is_map(Selected) ->
    emqx_rule_metrics:inc_actions_success(?bound_v('ActId', Envs)).

format_msg([], Data) ->
    emqx_json:encode(Data);
format_msg(Tokens, Data) ->
     emqx_rule_utils:proc_tmpl(Tokens, Data).

%% -1 for old version.
to_qos(<<"-1">>) -> -1;
to_qos(-1) -> -1;
to_qos(TargetQoS) ->
    try
        qos(TargetQoS)
    catch _:_ ->
        %% Use placeholder.
        case emqx_rule_utils:preproc_tmpl(TargetQoS) of
            Tmpl = [{var, _}] ->
                Tmpl;
            _BadQoS ->
                error({bad_qos, TargetQoS})
        end
    end.

get_qos(-1, _Data, Default) -> Default;
get_qos(TargetQoS, Data, _Default) ->
    qos(emqx_rule_utils:replace_var(TargetQoS, Data)).

assert_topic_valid(T) ->
    case emqx_topic:wildcard(T) of
        true -> throw({?BAD_TOPIC_WITH_WILDCARD, T});
        false -> T
    end.

qos(<<"0">>) ->  0;
qos(<<"1">>) ->  1;
qos(<<"2">>) ->  2;
qos(0) ->        0;
qos(1) ->        1;
qos(2) ->        2;
qos(BadQoS) -> error({bad_qos, BadQoS}).

to_retain(TargetRetain) ->
    try
        retain(TargetRetain)
    catch _:_ ->
        %% Use placeholder.
        case emqx_rule_utils:preproc_tmpl(TargetRetain) of
            Tmpl = [{var, _}] ->
                Tmpl;
            _BadRetain ->
                error({bad_retain, TargetRetain})
        end
    end.

get_retain(TargetRetain, Data) ->
    retain(emqx_rule_utils:replace_var(TargetRetain, Data)).

retain(true) ->        true;
retain(false) ->       false;
retain(<<"true">>) ->  true;
retain(<<"false">>) -> false;
retain(<<"1">>) ->     true;
retain(<<"0">>) ->     false;
retain(1) ->           true;
retain(0) ->           false;
retain(BadRetain) -> error({bad_retain, BadRetain}).
