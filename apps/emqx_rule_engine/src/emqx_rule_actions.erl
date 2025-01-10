%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqtt/include/emqtt.hrl").

%% APIs
-export([parse_action/1]).

%% callbacks of emqx_rule_action
-export([pre_process_action_args/2]).

%% action functions
-export([
    console/3,
    republish/3
]).

-optional_callbacks([pre_process_action_args/2]).

-callback pre_process_action_args(FuncName :: atom(), action_fun_args()) -> action_fun_args().

-define(ORIGINAL_USER_PROPERTIES, original).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
parse_action(BridgeId) when is_binary(BridgeId) ->
    {Type, Name} = emqx_bridge_resource:parse_bridge_id(BridgeId),
    case emqx_bridge_v2:is_bridge_v2_type(Type) of
        true ->
            %% Could be an old bridge V1 type that should be converted to a V2 type
            try emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(Type) of
                BridgeV2Type ->
                    {bridge_v2, BridgeV2Type, Name}
            catch
                _:_ ->
                    %% We got a bridge v2 type that is not also a bridge v1
                    %% type
                    {bridge_v2, Type, Name}
            end;
        false ->
            {bridge, Type, Name, emqx_bridge_resource:resource_id(Type, Name)}
    end;
parse_action(#{function := ActionFunc} = Action) ->
    {Mod, Func} = parse_action_func(ActionFunc),
    Res = #{mod => Mod, func => Func},
    %% builtin_action_console don't have args field.
    %% Attempting to save args to the console action config could cause validation issues
    case Action of
        #{args := Args} ->
            Res#{args => pre_process_args(Mod, Func, Args)};
        _ ->
            Res
    end.

%%--------------------------------------------------------------------
%% callbacks of emqx_rule_action
%%--------------------------------------------------------------------
pre_process_action_args(
    republish,
    #{
        topic := Topic,
        qos := QoS,
        retain := Retain,
        payload := Payload,
        mqtt_properties := MQTTProperties,
        user_properties := UserProperties,
        direct_dispatch := DirectDispatch
    } = Args
) ->
    Args#{
        preprocessed_tmpl => #{
            topic => emqx_template:parse(Topic),
            qos => parse_simple_var(QoS),
            retain => parse_simple_var(Retain),
            payload => parse_payload(Payload),
            mqtt_properties => parse_mqtt_properties(MQTTProperties),
            user_properties => parse_user_properties(UserProperties),
            direct_dispatch => parse_simple_var(DirectDispatch)
        }
    };
pre_process_action_args(_, Args) ->
    Args.

%%--------------------------------------------------------------------
%% action functions
%%--------------------------------------------------------------------
-spec console(map(), map(), map()) -> any().
console(Selected, #{metadata := #{rule_id := RuleId}} = Envs, _Args) ->
    case logger:get_process_metadata() of
        #{action_id := ActionID} ->
            emqx_trace:rendered_action_template(
                ActionID,
                #{
                    selected => Selected,
                    environment => Envs
                }
            );
        _ ->
            %% We may not have an action ID in the metadata if this is called
            %% from a test case or similar
            ok
    end,
    ?ULOG(
        "[rule action] ~ts~n"
        "\tAction Data: ~p~n"
        "\tEnvs: ~p~n",
        [RuleId, Selected, Envs]
    ).

republish(
    _Selected,
    #{
        topic := Topic,
        headers := #{republish_by := RuleId},
        metadata := #{rule_id := RuleId}
    },
    _Args
) ->
    ?SLOG(
        error,
        #{
            msg => "recursive_republish_detected",
            topic => Topic,
            rule_id => RuleId
        },
        #{tag => ?TAG}
    );
republish(
    Selected,
    #{metadata := #{rule_id := RuleId}} = Env,
    #{
        preprocessed_tmpl := #{
            qos := QoSTemplate,
            retain := RetainTemplate,
            topic := TopicTemplate,
            payload := PayloadTemplate,
            mqtt_properties := MQTTPropertiesTemplate,
            user_properties := UserPropertiesTemplate,
            direct_dispatch := DirectDispatchTemplate
        }
    }
) ->
    % NOTE: rendering missing bindings as string "undefined"
    {TopicString, _Errors1} = render_template(TopicTemplate, Selected),
    {PayloadString, _Errors2} = render_template(PayloadTemplate, Selected),
    Topic = iolist_to_binary(TopicString),
    Payload = iolist_to_binary(PayloadString),
    QoS = render_simple_var(QoSTemplate, Selected, 0),
    Retain = render_simple_var(RetainTemplate, Selected, false),
    DirectDispatch = render_simple_var(DirectDispatchTemplate, Selected, false),
    %% 'flags' is set for message re-publishes or message related
    %% events such as message.acked and message.dropped
    Flags0 = maps:get(flags, Env, #{}),
    Flags = Flags0#{retain => Retain},
    PubProps0 = render_pub_props(UserPropertiesTemplate, Selected, Env),
    MQTTProps = render_mqtt_properties(MQTTPropertiesTemplate, Selected, Env),
    PubProps = maps:merge(PubProps0, MQTTProps),
    TraceInfo = #{
        flags => Flags,
        topic => Topic,
        payload => Payload,
        pub_props => PubProps,
        direct_dispatch => DirectDispatch
    },
    case logger:get_process_metadata() of
        #{action_id := ActionID} ->
            emqx_trace:rendered_action_template(ActionID, TraceInfo);
        _ ->
            %% We may not have an action ID in the metadata if this is called
            %% from a test case or similar
            ok
    end,
    ?TRACE(
        "RULE",
        "republish_message",
        TraceInfo
    ),
    safe_publish(RuleId, Topic, QoS, Flags, Payload, PubProps, DirectDispatch).

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------
parse_action_func(ActionFunc) ->
    {Mod, Func} = get_action_mod_func(ActionFunc),
    assert_function_supported(Mod, Func),
    {Mod, Func}.

get_action_mod_func(ActionFunc) when is_atom(ActionFunc) ->
    {emqx_rule_actions, ActionFunc};
get_action_mod_func(ActionFunc) when is_binary(ActionFunc) ->
    ToAtom = fun(Bin) ->
        try binary_to_existing_atom(Bin) of
            Atom -> Atom
        catch
            error:badarg -> validation_error(unknown_action_function)
        end
    end,
    case string:split(ActionFunc, ":", all) of
        [Func1] -> {emqx_rule_actions, ToAtom(Func1)};
        [Mod1, Func1] -> {ToAtom(Mod1), ToAtom(Func1)};
        _ -> validation_error(invalid_action_function)
    end.

assert_function_supported(Mod, Func) ->
    case erlang:function_exported(Mod, Func, 3) of
        true -> ok;
        false -> validation_error(action_function_not_supported)
    end.

-spec validation_error(any()) -> no_return().
validation_error(Reason) ->
    throw(#{kind => validation_error, reason => Reason}).

pre_process_args(Mod, Func, Args) ->
    case erlang:function_exported(Mod, pre_process_action_args, 2) of
        true -> Mod:pre_process_action_args(Func, Args);
        false -> Args
    end.

safe_publish(RuleId, Topic, QoS, Flags, Payload, PubProps, DirectDispatch) ->
    Msg = #message{
        id = emqx_guid:gen(),
        qos = QoS,
        from = RuleId,
        flags = Flags,
        headers = #{
            republish_by => RuleId,
            properties => emqx_utils:pub_props_to_packet(PubProps)
        },
        topic = Topic,
        payload = Payload,
        timestamp = erlang:system_time(millisecond)
    },
    case
        emqx_broker:safe_publish(Msg, #{
            bypass_hook => DirectDispatch, hook_prohibition_as_error => true
        })
    of
        Routes when is_list(Routes) ->
            emqx_metrics:inc_msg(Msg),
            ok;
        disconnect ->
            error;
        {blocked, _Msg} ->
            error
    end.

parse_simple_var(Data) when is_binary(Data) ->
    emqx_template:parse(Data);
parse_simple_var(Data) ->
    {const, Data}.

parse_payload(Payload) ->
    case string:is_empty(Payload) of
        false -> emqx_template:parse(Payload);
        true -> emqx_template:parse("${.}")
    end.

parse_mqtt_properties(MQTTPropertiesTemplate) ->
    maps:map(
        fun(_Key, V) -> emqx_template:parse(V) end,
        MQTTPropertiesTemplate
    ).

parse_user_properties(<<"${pub_props.'User-Property'}">>) ->
    %% keep the original
    %% avoid processing this special variable because
    %% we do not want to force users to select the value
    %% the value will be taken from Env.pub_props directly
    ?ORIGINAL_USER_PROPERTIES;
parse_user_properties(<<"${.pub_props.'User-Property'}">>) ->
    %% keep the original
    %% avoid processing this special variable because
    %% we do not want to force users to select the value
    %% the value will be taken from Env.pub_props directly
    ?ORIGINAL_USER_PROPERTIES;
parse_user_properties(<<"${", _/binary>> = V) ->
    %% use a variable
    emqx_template:parse(V);
parse_user_properties(_) ->
    %% invalid, discard
    undefined.

render_template(Template, Bindings) ->
    emqx_template:render(Template, {emqx_jsonish, Bindings}).

render_simple_var([{var, _Name, Accessor}], Data, Default) ->
    case emqx_jsonish:lookup(Accessor, Data) of
        {ok, Var} -> Var;
        %% cannot find the variable from Data
        {error, _} -> Default
    end;
render_simple_var({const, Val}, _Data, _Default) ->
    Val.

render_pub_props(UserPropertiesTemplate, Selected, Env) ->
    UserProperties =
        case UserPropertiesTemplate of
            ?ORIGINAL_USER_PROPERTIES ->
                maps:get('User-Property', maps:get(pub_props, Env, #{}), #{});
            undefined ->
                #{};
            _ ->
                render_simple_var(UserPropertiesTemplate, Selected, #{})
        end,
    #{'User-Property' => UserProperties}.

%%

-define(BADPROP(K, REASON, ENV, DATA),
    ?SLOG(
        debug,
        ?MAPPEND(DATA, #{
            tag => ?TAG,
            msg => "bad_mqtt_property_value_ignored",
            rule_id => emqx_utils_maps:deep_get([metadata, rule_id], ENV, undefined),
            reason => REASON,
            property => K
        })
    )
).

render_mqtt_properties(MQTTPropertiesTemplate, Selected, Env) ->
    MQTTProperties =
        maps:map(
            fun(K, Template) ->
                {V, Errors} = render_template(Template, Selected),
                case Errors of
                    [] ->
                        ok;
                    Errors ->
                        ?BADPROP(K, Errors, Env, #{selected => Selected})
                end,
                iolist_to_binary(V)
            end,
            MQTTPropertiesTemplate
        ),
    coerce_properties_values(MQTTProperties, Env).

ensure_int(B) when is_binary(B) ->
    try
        binary_to_integer(B)
    catch
        error:badarg ->
            throw(bad_integer)
    end;
ensure_int(I) when is_integer(I) ->
    I.

coerce_properties_values(MQTTProperties, Env) ->
    maps:filtermap(
        fun(K, V) ->
            try
                {true, encode_mqtt_property(K, V)}
            catch
                throw:Reason ->
                    ?BADPROP(K, Reason, Env, #{value => V}),
                    false;
                Kind:Reason:Stacktrace ->
                    ?BADPROP(K, Reason, Env, #{
                        value => V,
                        exception => Kind,
                        stacktrace => Stacktrace
                    }),
                    false
            end
        end,
        MQTTProperties
    ).

%% Note: currently we do not support `Topic-Alias', which would need to be encoded as an
%% int.
encode_mqtt_property('Payload-Format-Indicator', V) -> ensure_int(V);
encode_mqtt_property('Message-Expiry-Interval', V) -> ensure_int(V);
encode_mqtt_property('Subscription-Identifier', V) -> ensure_int(V);
%% note: `emqx_placeholder:proc_tmpl/2' currently always return a binary.
encode_mqtt_property(_Prop, V) when is_binary(V) -> V.
