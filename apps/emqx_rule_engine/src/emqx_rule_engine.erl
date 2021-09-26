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

-module(emqx_rule_engine).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ create_rule/1
        , update_rule/1
        , delete_rule/1
        ]).

-export_type([rule/0]).

-type(rule() :: #rule{}).

-define(T_RETRY, 60000).

%%------------------------------------------------------------------------------
%% APIs for rules and resources
%%------------------------------------------------------------------------------

-spec create_rule(map()) -> {ok, rule()} | {error, term()}.
create_rule(Params = #{id := RuleId}) ->
    case emqx_rule_registry:get_rule(RuleId) of
        not_found -> do_create_rule(Params);
        {ok, _} -> {error, {already_exists, RuleId}}
    end.

-spec update_rule(map()) -> {ok, rule()} | {error, term()}.
update_rule(Params = #{id := RuleId}) ->
    case delete_rule(RuleId) of
        ok -> do_create_rule(Params);
        Error -> Error
    end.

-spec(delete_rule(RuleId :: rule_id()) -> ok | {error, term()}).
delete_rule(RuleId) ->
    case emqx_rule_registry:get_rule(RuleId) of
        {ok, Rule} ->
            ok = emqx_rule_registry:remove_rule(Rule),
            _ = emqx_plugin_libs_rule:cluster_call(emqx_rule_metrics, clear_rule_metrics, [RuleId]),
            ok;
        not_found ->
            {error, not_found}
    end.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

%% The pattern {'ok', Select} can never match the type {'error',{_,[{_,_,_,_}]}}.
%% probably due to stack depth, or inlines.
-dialyzer({nowarn_function, [do_create_rule/1, parse_outputs/1, do_parse_outputs/1,
    pre_process_repub_args/1, preproc_vars/1]}).

do_create_rule(Params = #{id := RuleId, sql := Sql, outputs := Outputs}) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Rule = #rule{
                id = RuleId,
                created_at = erlang:system_time(millisecond),
                info = #{
                    enabled => maps:get(enabled, Params, true),
                    sql => Sql,
                    from => emqx_rule_sqlparser:select_from(Select),
                    outputs => parse_outputs(Outputs),
                    description => maps:get(description, Params, ""),
                    %% -- calculated fields:
                    is_foreach => emqx_rule_sqlparser:select_is_foreach(Select),
                    fields => emqx_rule_sqlparser:select_fields(Select),
                    doeach => emqx_rule_sqlparser:select_doeach(Select),
                    incase => emqx_rule_sqlparser:select_incase(Select),
                    conditions => emqx_rule_sqlparser:select_where(Select)
                    %% -- calculated fields end
                }
            },
            ok = emqx_rule_registry:add_rule(Rule),
            _ = emqx_plugin_libs_rule:cluster_call(emqx_rule_metrics, create_rule_metrics, [RuleId]),
            {ok, Rule};
        {error, Reason} -> {error, Reason}
    end.

parse_outputs(Outputs) ->
    [do_parse_outputs(Out) || Out <- Outputs].

do_parse_outputs(#{type := bridge, target := ChId}) ->
    #{type => bridge, target => ChId};
do_parse_outputs(#{type := builtin, target := Repub, args := Args})
        when Repub == republish; Repub == <<"republish">> ->
    #{type => builtin, target => republish, args => pre_process_repub_args(Args)};
do_parse_outputs(#{type := Type, target := Name} = Output)
        when Type == func; Type == builtin ->
    #{type => Type, target => Name, args => maps:get(args, Output, #{})}.

pre_process_repub_args(#{<<"topic">> := Topic} = Args) ->
    QoS = maps:get(<<"qos">>, Args, <<"${qos}">>),
    Retain = maps:get(<<"retain">>, Args, <<"${retain}">>),
    Payload = maps:get(<<"payload">>, Args, <<"${payload}">>),
    #{topic => Topic, qos => QoS, payload => Payload, retain => Retain,
      preprocessed_tmpl => #{
          topic => emqx_plugin_libs_rule:preproc_tmpl(Topic),
          qos => preproc_vars(QoS),
          retain => preproc_vars(Retain),
          payload => emqx_plugin_libs_rule:preproc_tmpl(Payload)
      }}.

preproc_vars(Data) when is_binary(Data) ->
    emqx_plugin_libs_rule:preproc_tmpl(Data);
preproc_vars(Data) ->
    Data.
