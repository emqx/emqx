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

-module(emqx_rule_engine_api).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/2, array/1]).

%% Swagger specs from hocon schema
-export([api_spec/0, paths/0, schema/1, namespace/0]).

%% API callbacks
-export(['/rule_events'/2, '/rule_test'/2, '/rules'/2, '/rules/:id'/2]).

-define(ERR_NO_RULE(ID), list_to_binary(io_lib:format("Rule ~ts Not Found", [(ID)]))).
-define(ERR_BADARGS(REASON),
        begin
            R0 = err_msg(REASON),
            <<"Bad Arguments: ", R0/binary>>
        end).
-define(CHECK_PARAMS(PARAMS, TAG, EXPR),
    case emqx_rule_api_schema:check_params(PARAMS, TAG) of
        {ok, CheckedParams} ->
            EXPR;
        {error, REASON} ->
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(REASON)}}
    end).
-define(METRICS(MATCH, PASS, FAIL, FAIL_EX, FAIL_NORES, O_TOTAL, O_FAIL, O_FAIL_OOS,
        O_FAIL_UNKNOWN, O_SUCC, RATE, RATE_MAX, RATE_5),
    #{
        'sql.matched' => MATCH,
        'sql.passed' => PASS,
        'sql.failed' => FAIL,
        'sql.failed.exception' => FAIL_EX,
        'sql.failed.no_result' => FAIL_NORES,
        'outputs.total' => O_TOTAL,
        'outputs.failed' => O_FAIL,
        'outputs.failed.out_of_service' => O_FAIL_OOS,
        'outputs.failed.unknown' => O_FAIL_UNKNOWN,
        'outputs.success' => O_SUCC,
        'sql.matched.rate' => RATE,
        'sql.matched.rate.max' => RATE_MAX,
        'sql.matched.rate.last5m' => RATE_5
    }).
-define(metrics(MATCH, PASS, FAIL, FAIL_EX, FAIL_NORES, O_TOTAL, O_FAIL, O_FAIL_OOS,
        O_FAIL_UNKNOWN, O_SUCC, RATE, RATE_MAX, RATE_5),
    #{
        'sql.matched' := MATCH,
        'sql.passed' := PASS,
        'sql.failed' := FAIL,
        'sql.failed.exception' := FAIL_EX,
        'sql.failed.no_result' := FAIL_NORES,
        'outputs.total' := O_TOTAL,
        'outputs.failed' := O_FAIL,
        'outputs.failed.out_of_service' := O_FAIL_OOS,
        'outputs.failed.unknown' := O_FAIL_UNKNOWN,
        'outputs.success' := O_SUCC,
        'sql.matched.rate' := RATE,
        'sql.matched.rate.max' := RATE_MAX,
        'sql.matched.rate.last5m' := RATE_5
    }).

namespace() -> "rule".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() -> ["/rule_events", "/rule_test", "/rules", "/rules/:id"].

error_schema(Code, Message) when is_atom(Code) ->
    emqx_dashboard_swagger:error_codes([Code], list_to_binary(Message)).

rule_creation_schema() ->
    ref(emqx_rule_api_schema, "rule_creation").

rule_test_schema() ->
    ref(emqx_rule_api_schema, "rule_test").

rule_info_schema() ->
    ref(emqx_rule_api_schema, "rule_info").

schema("/rules") ->
    #{
        operationId => '/rules',
        get => #{
            tags => [<<"rules">>],
            description => <<"List all rules">>,
            summary => <<"List Rules">>,
            responses => #{
                200 => mk(array(rule_info_schema()), #{desc => "List of rules"})
            }},
        post => #{
            tags => [<<"rules">>],
            description => <<"Create a new rule using given Id">>,
            summary => <<"Create a Rule">>,
            requestBody => rule_creation_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                201 => rule_info_schema()
            }}
    };

schema("/rule_events") ->
    #{
        operationId => '/rule_events',
        get => #{
            tags => [<<"rules">>],
            description => <<"List all events can be used in rules">>,
            summary => <<"List Events">>,
            responses => #{
                200 => mk(ref(emqx_rule_api_schema, "rule_events"), #{})
            }
        }
    };

schema("/rules/:id") ->
    #{
        operationId => '/rules/:id',
        get => #{
            tags => [<<"rules">>],
            description => <<"Get a rule by given Id">>,
            summary => <<"Get a Rule">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                200 => rule_info_schema()
            }
        },
        put => #{
            tags => [<<"rules">>],
            description => <<"Update a rule by given Id to all nodes in the cluster">>,
            summary => <<"Update a Rule">>,
            parameters => param_path_id(),
            requestBody => rule_creation_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                200 => rule_info_schema()
            }
        },
        delete => #{
            tags => [<<"rules">>],
            description => <<"Delete a rule by given Id from all nodes in the cluster">>,
            summary => <<"Delete a Rule">>,
            parameters => param_path_id(),
            responses => #{
                204 => <<"Delete rule successfully">>
            }
        }
    };

schema("/rule_test") ->
    #{
        operationId => '/rule_test',
        post => #{
            tags => [<<"rules">>],
            description => <<"Test a rule">>,
            summary => <<"Test a Rule">>,
            requestBody => rule_test_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                412 => error_schema('NOT_MATCH', "SQL Not Match"),
                200 => <<"Rule Test Pass">>
            }
        }
    }.

param_path_id() ->
    [{id, mk(binary(), #{in => path, example => <<"my_rule_id">>})}].

%%------------------------------------------------------------------------------
%% Rules API
%%------------------------------------------------------------------------------

%% To get around the hocon bug, we replace crlf with spaces
replace_sql_clrf(#{ <<"sql">> := SQL } = Params) ->
    NewSQL = re:replace(SQL, "[\r\n]", " ", [{return, binary}, global]),
    Params#{<<"sql">> => NewSQL}.

'/rule_events'(get, _Params) ->
    {200, emqx_rule_events:event_info()}.

'/rules'(get, _Params) ->
    Records = emqx_rule_engine:get_rules_ordered_by_ts(),
    {200, format_rule_resp(Records)};

'/rules'(post, #{body := Params0}) ->
    case maps:get(<<"id">>, Params0, list_to_binary(emqx_misc:gen_id(8))) of
        <<>> ->
            {400, #{code => 'BAD_REQUEST', message => <<"empty rule id is not allowed">>}};
        Id ->
            Params = filter_out_request_body(replace_sql_clrf(Params0)),
            ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
            case emqx_rule_engine:get_rule(Id) of
                {ok, _Rule} ->
                    {400, #{code => 'BAD_REQUEST', message => <<"rule id already exists">>}};
                not_found ->
                    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
                        {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
                            [Rule] = get_one_rule(AllRules, Id),
                            {201, format_rule_resp(Rule)};
                        {error, Reason} ->
                            ?SLOG(error, #{msg => "create_rule_failed",
                                        id => Id, reason => Reason}),
                            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
                    end
            end
    end.

'/rule_test'(post, #{body := Params}) ->
    ?CHECK_PARAMS(Params, rule_test, case emqx_rule_sqltester:test(CheckedParams) of
        {ok, Result} -> {200, Result};
        {error, {parse_error, Reason}} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        {error, nomatch} -> {412, #{code => 'NOT_MATCH', message => <<"SQL Not Match">>}}
    end).

'/rules/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, Rule} ->
            {200, format_rule_resp(Rule)};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;

'/rules/:id'(put, #{bindings := #{id := Id}, body := Params0}) ->
    Params = filter_out_request_body(Params0),
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
        {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
            [Rule] = get_one_rule(AllRules, Id),
            {200, format_rule_resp(Rule)};
        {error, Reason} ->
            ?SLOG(error, #{msg => "update_rule_failed",
                           id => Id, reason => Reason}),
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
    end;

'/rules/:id'(delete, #{bindings := #{id := Id}}) ->
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx_conf:remove(ConfPath, #{override_to => cluster}) of
        {ok, _} -> {204};
        {error, Reason} ->
            ?SLOG(error, #{msg => "delete_rule_failed",
                           id => Id, reason => Reason}),
            {500, #{code => 'INTERNAL_ERROR', message => ?ERR_BADARGS(Reason)}}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
err_msg(Msg) ->
    list_to_binary(io_lib:format("~0p", [Msg])).


format_rule_resp(Rules) when is_list(Rules) ->
    [format_rule_resp(R) || R <- Rules];

format_rule_resp(#{ id := Id, name := Name,
                    created_at := CreatedAt,
                    from := Topics,
                    outputs := Output,
                    sql := SQL,
                    enable := Enable,
                    description := Descr}) ->
    NodeMetrics = get_rule_metrics(Id),
    #{id => Id,
      name => Name,
      from => Topics,
      outputs => format_output(Output),
      sql => SQL,
      metrics => aggregate_metrics(NodeMetrics),
      node_metrics => NodeMetrics,
      enable => Enable,
      created_at => format_datetime(CreatedAt, millisecond),
      description => Descr
     }.

format_datetime(Timestamp, Unit) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, Unit}])).

format_output(Outputs) ->
    [do_format_output(Out) || Out <- Outputs].

do_format_output(#{mod := Mod, func := Func, args := Args}) ->
    #{function => printable_function_name(Mod, Func),
      args => maps:remove(preprocessed_tmpl, Args)};
do_format_output(BridgeChannelId) when is_binary(BridgeChannelId) ->
    BridgeChannelId.

printable_function_name(emqx_rule_outputs, Func) ->
    Func;
printable_function_name(Mod, Func) ->
    list_to_binary(lists:concat([Mod,":",Func])).

get_rule_metrics(Id) ->
    Format = fun (Node, #{
            counters :=
                #{'sql.matched' := Matched, 'sql.passed' := Passed, 'sql.failed' := Failed,
                 'sql.failed.exception' := FailedEx,
                 'sql.failed.no_result' := FailedNoRes,
                 'outputs.total' := OTotal,
                 'outputs.failed' := OFailed,
                 'outputs.failed.out_of_service' := OFailedOOS,
                 'outputs.failed.unknown' := OFailedUnknown,
                 'outputs.success' := OFailedSucc
                 },
            rate :=
                #{'sql.matched' :=
                    #{current := Current, max := Max, last5m := Last5M}
                 }}) ->
        #{ metrics => ?METRICS(Matched, Passed, Failed, FailedEx, FailedNoRes,
            OTotal, OFailed, OFailedOOS, OFailedUnknown, OFailedSucc, Current, Max, Last5M)
         , node => Node
         }
    end,
    [Format(Node, emqx_plugin_libs_proto_v1:get_metrics(Node, rule_metrics, Id))
     || Node <- mria_mnesia:running_nodes()].

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?METRICS(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    lists:foldl(fun
        (#{metrics := ?metrics(Match1, Passed1, Failed1, FailedEx1, FailedNoRes1,
             OTotal1, OFailed1, OFailedOOS1, OFailedUnknown1, OFailedSucc1,
             Rate1, RateMax1, Rate5m1)},
          ?metrics(Match0, Passed0, Failed0, FailedEx0, FailedNoRes0,
             OTotal0, OFailed0, OFailedOOS0, OFailedUnknown0, OFailedSucc0,
             Rate0, RateMax0, Rate5m0)) ->
        ?METRICS(Match1 + Match0, Passed1 + Passed0, Failed1 + Failed0,
             FailedEx1 + FailedEx0, FailedNoRes1 + FailedNoRes0,
             OTotal1 + OTotal0, OFailed1 + OFailed0,
             OFailedOOS1 + OFailedOOS0,
             OFailedUnknown1 + OFailedUnknown0,
             OFailedSucc1 + OFailedSucc0,
             Rate1 + Rate0, RateMax1 + RateMax0, Rate5m1 + Rate5m0)
        end, InitMetrics, AllMetrics).

get_one_rule(AllRules, Id) ->
    [R || R = #{id := Id0} <- AllRules, Id0 == Id].

filter_out_request_body(Conf) ->
    ExtraConfs = [<<"id">>, <<"status">>, <<"node_status">>, <<"node_metrics">>,
        <<"metrics">>, <<"node">>],
    maps:without(ExtraConfs, Conf).
