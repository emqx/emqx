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

-export([ load_rules/0
        ]).

-export([ create_rule/1
        , update_rule/1
        , delete_rule/1
        ]).

%%------------------------------------------------------------------------------
%% APIs for rules and resources
%%------------------------------------------------------------------------------

-spec load_rules() -> ok.
load_rules() ->
    lists:foreach(fun({Id, Rule}) ->
            {ok, _} = create_rule(Rule#{id => Id})
        end, maps:to_list(emqx:get_config([rule_engine, rules], #{}))).

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
            emqx_rule_registry:remove_rule(Rule);
        not_found ->
            {error, not_found}
    end.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

do_create_rule(Params = #{id := RuleId, sql := Sql, outputs := Outputs}) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Rule = #{
                id => RuleId,
                created_at => erlang:system_time(millisecond),
                enabled => maps:get(enabled, Params, true),
                sql => Sql,
                outputs => parse_outputs(Outputs),
                description => maps:get(description, Params, ""),
                %% -- calculated fields:
                from => emqx_rule_sqlparser:select_from(Select),
                is_foreach => emqx_rule_sqlparser:select_is_foreach(Select),
                fields => emqx_rule_sqlparser:select_fields(Select),
                doeach => emqx_rule_sqlparser:select_doeach(Select),
                incase => emqx_rule_sqlparser:select_incase(Select),
                conditions => emqx_rule_sqlparser:select_where(Select)
                %% -- calculated fields end
            },
            ok = emqx_rule_registry:add_rule(Rule),
            {ok, Rule};
        {error, Reason} -> {error, Reason}
    end.

parse_outputs(Outputs) ->
    [do_parse_outputs(Out) || Out <- Outputs].

do_parse_outputs(#{function := Repub, args := Args})
        when Repub == republish; Repub == <<"republish">> ->
    #{function => republish, args => emqx_rule_outputs:pre_process_repub_args(Args)};
do_parse_outputs(#{function := Func} = Output) ->
    #{function => parse_output_func(Func), args => maps:get(args, Output, #{})};
do_parse_outputs(BridgeChannelId) when is_binary(BridgeChannelId) ->
    BridgeChannelId.

parse_output_func(FuncName) when is_atom(FuncName) ->
    FuncName;
parse_output_func(BinFunc) when is_binary(BinFunc) ->
    try binary_to_existing_atom(BinFunc) of
        Func -> emqx_rule_outputs:assert_builtin_output(Func)
    catch
        error:badarg -> error({unknown_builtin_function, BinFunc})
    end;
parse_output_func(Func) when is_function(Func) ->
    Func.
