%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").

%% APIs
-export([parse_output/1]).

%% callbacks of emqx_rule_output
-export([pre_process_output_args/2]).

%% output functions
-export([
    console/3,
    republish/3
]).

-optional_callbacks([pre_process_output_args/2]).

-callback pre_process_output_args(FuncName :: atom(), output_fun_args()) -> output_fun_args().

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
parse_output(#{function := OutputFunc} = Output) ->
    {Mod, Func} = parse_output_func(OutputFunc),
    #{
        mod => Mod,
        func => Func,
        args => pre_process_args(Mod, Func, maps:get(args, Output, #{}))
    }.

%%--------------------------------------------------------------------
%% callbacks of emqx_rule_output
%%--------------------------------------------------------------------
pre_process_output_args(
    republish,
    #{
        topic := Topic,
        qos := QoS,
        retain := Retain,
        payload := Payload
    } = Args
) ->
    Args#{
        preprocessed_tmpl => #{
            topic => emqx_plugin_libs_rule:preproc_tmpl(Topic),
            qos => preproc_vars(QoS),
            retain => preproc_vars(Retain),
            payload => emqx_plugin_libs_rule:preproc_tmpl(Payload)
        }
    };
pre_process_output_args(_, Args) ->
    Args.

%%--------------------------------------------------------------------
%% output functions
%%--------------------------------------------------------------------
-spec console(map(), map(), map()) -> any().
console(Selected, #{metadata := #{rule_id := RuleId}} = Envs, _Args) ->
    ?ULOG(
        "[rule output] ~ts~n"
        "\tOutput Data: ~p~n"
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
    ?SLOG(error, #{msg => "recursive_republish_detected", topic => Topic});
%% republish a PUBLISH message
republish(
    Selected,
    #{flags := Flags, metadata := #{rule_id := RuleId}},
    #{
        preprocessed_tmpl := #{
            qos := QoSTks,
            retain := RetainTks,
            topic := TopicTks,
            payload := PayloadTks
        }
    }
) ->
    Topic = emqx_plugin_libs_rule:proc_tmpl(TopicTks, Selected),
    Payload = format_msg(PayloadTks, Selected),
    QoS = replace_simple_var(QoSTks, Selected, 0),
    Retain = replace_simple_var(RetainTks, Selected, false),
    ?TRACE("RULE", "republish_message", #{topic => Topic, payload => Payload}),
    safe_publish(RuleId, Topic, QoS, Flags#{retain => Retain}, Payload);
%% in case this is a "$events/" event
republish(
    Selected,
    #{metadata := #{rule_id := RuleId}},
    #{
        preprocessed_tmpl := #{
            qos := QoSTks,
            retain := RetainTks,
            topic := TopicTks,
            payload := PayloadTks
        }
    }
) ->
    Topic = emqx_plugin_libs_rule:proc_tmpl(TopicTks, Selected),
    Payload = format_msg(PayloadTks, Selected),
    QoS = replace_simple_var(QoSTks, Selected, 0),
    Retain = replace_simple_var(RetainTks, Selected, false),
    ?TRACE("RULE", "republish_message_with_flags", #{topic => Topic, payload => Payload}),
    safe_publish(RuleId, Topic, QoS, #{retain => Retain}, Payload).

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------
parse_output_func(OutputFunc) ->
    {Mod, Func} = get_output_mod_func(OutputFunc),
    assert_function_supported(Mod, Func),
    {Mod, Func}.

get_output_mod_func(OutputFunc) when is_atom(OutputFunc) ->
    {emqx_rule_outputs, OutputFunc};
get_output_mod_func(OutputFunc) when is_binary(OutputFunc) ->
    ToAtom = fun(Bin) ->
        try binary_to_existing_atom(Bin) of
            Atom -> Atom
        catch
            error:badarg -> error({unknown_output_function, OutputFunc})
        end
    end,
    case string:split(OutputFunc, ":", all) of
        [Func1] -> {emqx_rule_outputs, ToAtom(Func1)};
        [Mod1, Func1] -> {ToAtom(Mod1), ToAtom(Func1)};
        _ -> error({invalid_output_function, OutputFunc})
    end.

assert_function_supported(Mod, Func) ->
    case erlang:function_exported(Mod, Func, 3) of
        true -> ok;
        false -> error({output_function_not_supported, Func})
    end.

pre_process_args(Mod, Func, Args) ->
    case erlang:function_exported(Mod, pre_process_output_args, 2) of
        true -> Mod:pre_process_output_args(Func, Args);
        false -> Args
    end.

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

preproc_vars(Data) when is_binary(Data) ->
    emqx_plugin_libs_rule:preproc_tmpl(Data);
preproc_vars(Data) ->
    Data.

replace_simple_var(Tokens, Data, Default) when is_list(Tokens) ->
    [Var] = emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => rawlist}),
    case Var of
        %% cannot find the variable from Data
        undefined -> Default;
        _ -> Var
    end;
replace_simple_var(Val, _Data, _Default) ->
    Val.

format_msg([], Selected) ->
    emqx_json:encode(Selected);
format_msg(Tokens, Selected) ->
    emqx_plugin_libs_rule:proc_tmpl(Tokens, Selected).
