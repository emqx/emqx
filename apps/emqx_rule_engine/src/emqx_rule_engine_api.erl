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

-module(emqx_rule_engine_api).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(minirest_api).

-export([api_spec/0]).

-export([ crud_rules/2
        , list_events/2
        , crud_rules_by_id/2
        , rule_test/2
        ]).

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
            {400, #{code => 'BAD_ARGS', message => ?ERR_BADARGS(REASON)}}
    end).

api_spec() ->
    {
        [ api_rules_list_create()
        , api_rules_crud()
        , api_rule_test()
        , api_events_list()
        ],
        []
    }.

api_rules_list_create() ->
    Metadata = #{
        get => #{
            description => <<"List all rules">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:array_schema(resp_schema(), <<"List rules successfully">>)}},
        post => #{
            description => <<"Create a new rule using given Id to all nodes in the cluster">>,
            'requestBody' => emqx_mgmt_util:schema(post_req_schema(), <<"Rule parameters">>),
            responses => #{
                <<"400">> =>
                    emqx_mgmt_util:error_schema(<<"Invalid Parameters">>, ['BAD_ARGS']),
                <<"201">> =>
                    emqx_mgmt_util:schema(resp_schema(), <<"Create rule successfully">>)}}
    },
    {"/rules", Metadata, crud_rules}.

api_events_list() ->
    Metadata = #{
        get => #{
            description => <<"List all events can be used in rules">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:array_schema(resp_schema(), <<"List events successfully">>)}}
    },
    {"/rule_events", Metadata, list_events}.

api_rules_crud() ->
    Metadata = #{
        get => #{
            description => <<"Get a rule by given Id">>,
            parameters => [param_path_id()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(<<"Rule not found">>, ['NOT_FOUND']),
                <<"200">> =>
                    emqx_mgmt_util:schema(resp_schema(), <<"Get rule successfully">>)}},
        put => #{
            description => <<"Create or update a rule by given Id to all nodes in the cluster">>,
            parameters => [param_path_id()],
            'requestBody' => emqx_mgmt_util:schema(put_req_schema(), <<"Rule parameters">>),
            responses => #{
                <<"400">> =>
                    emqx_mgmt_util:error_schema(<<"Invalid Parameters">>, ['BAD_ARGS']),
                <<"200">> =>
                    emqx_mgmt_util:schema(resp_schema(),
                                          <<"Create or update rule successfully">>)}},
        delete => #{
            description => <<"Delete a rule by given Id from all nodes in the cluster">>,
            parameters => [param_path_id()],
            responses => #{
                <<"204">> =>
                    emqx_mgmt_util:schema(<<"Delete rule successfully">>)}}
    },
    {"/rules/:id", Metadata, crud_rules_by_id}.

api_rule_test() ->
    Metadata = #{
        post => #{
            description => <<"Test a rule">>,
            'requestBody' => emqx_mgmt_util:schema(rule_test_req_schema(), <<"Rule parameters">>),
            responses => #{
                <<"400">> =>
                    emqx_mgmt_util:error_schema(<<"Invalid Parameters">>, ['BAD_ARGS']),
                <<"412">> =>
                    emqx_mgmt_util:error_schema(<<"SQL Not Match">>, ['NOT_MATCH']),
                <<"200">> =>
                    emqx_mgmt_util:schema(rule_test_resp_schema(), <<"Rule Test Pass">>)}}
    },
    {"/rule_test", Metadata, rule_test}.

put_req_schema() ->
    #{type => object,
      properties => #{
        sql => #{
            description => <<"The SQL">>,
            type => string,
            example => <<"SELECT * from \"t/1\"">>
        },
        enable => #{
            description => <<"Enable or disable the rule">>,
            type => boolean,
            example => true
        },
        outputs => #{
            description => <<"The outputs of the rule">>,
            type => array,
            items => #{
                'oneOf' => [
                    #{
                        type => string,
                        example => <<"channel_id_of_my_bridge">>,
                        description => <<"The channel id of an emqx bridge">>
                    },
                    #{
                        type => object,
                        properties => #{
                            function => #{
                                type => string,
                                example => <<"console">>
                            }
                        }
                    }
                ]
            }
        },
        description => #{
            description => <<"The description for the rule">>,
            type => string,
            example => <<"A simple rule that handles MQTT messages from topic \"t/1\"">>
        }
      }
    }.

post_req_schema() ->
    Req = #{properties := Prop} = put_req_schema(),
    Req#{properties => Prop#{
        id => #{
            description => <<"The Id for the rule">>,
            example => <<"my_rule">>,
            type => string
        }
    }}.

resp_schema() ->
    Req = #{properties := Prop} = put_req_schema(),
    Req#{properties => Prop#{
        id => #{
            description => <<"The Id for the rule">>,
            type => string
        },
        created_at => #{
            description => <<"The time that this rule was created, in rfc3339 format">>,
            type => string,
            example => <<"2021-09-18T13:57:29+08:00">>
        }
    }}.

rule_test_req_schema() ->
    #{type => object, properties => #{
        sql => #{
            description => <<"The SQL">>,
            type => string,
            example => <<"SELECT * from \"t/1\"">>
        },
        context => #{
            type => object,
            properties => #{
                event_type => #{
                    description => <<"Event Type">>,
                    type => string,
                    enum => [<<"message_publish">>, <<"message_acked">>, <<"message_delivered">>,
                        <<"message_dropped">>, <<"session_subscribed">>, <<"session_unsubscribed">>,
                        <<"client_connected">>, <<"client_disconnected">>],
                    example => <<"message_publish">>
                },
                clientid => #{
                    description => <<"The Client ID">>,
                    type => string,
                    example => <<"\"c_emqx\"">>
                },
                topic => #{
                    description => <<"The Topic">>,
                    type => string,
                    example => <<"t/1">>
                }
            }
        }
    }}.

rule_test_resp_schema() ->
    #{type => object}.

param_path_id() ->
    #{
        name => id,
        in => path,
        schema => #{type => string},
        required => true
    }.

%%------------------------------------------------------------------------------
%% Rules API
%%------------------------------------------------------------------------------

list_events(#{}, _Params) ->
    {200, emqx_rule_events:event_info()}.

crud_rules(get, _Params) ->
    Records = emqx_rule_engine:get_rules_ordered_by_ts(),
    {200, format_rule_resp(Records)};

crud_rules(post, #{body := #{<<"id">> := Id} = Params}) ->
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx_rule_engine:get_rule(Id) of
        {ok, _Rule} ->
            {400, #{code => 'BAD_ARGS', message => <<"rule id already exists">>}};
        not_found ->
            case emqx:update_config(ConfPath, maps:remove(<<"id">>, Params), #{}) of
                {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
                    [Rule] = get_one_rule(AllRules, Id),
                    {201, format_rule_resp(Rule)};
                {error, Reason} ->
                    ?SLOG(error, #{msg => "create_rule_failed",
                                   id => Id, reason => Reason}),
                    {400, #{code => 'BAD_ARGS', message => ?ERR_BADARGS(Reason)}}
            end
    end.

rule_test(post, #{body := Params}) ->
    ?CHECK_PARAMS(Params, rule_test, case emqx_rule_sqltester:test(CheckedParams) of
        {ok, Result} -> {200, Result};
        {error, nomatch} -> {412, #{code => 'NOT_MATCH', message => <<"SQL Not Match">>}}
    end).

crud_rules_by_id(get, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, Rule} ->
            {200, format_rule_resp(Rule)};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;

crud_rules_by_id(put, #{bindings := #{id := Id}, body := Params}) ->
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx:update_config(ConfPath, maps:remove(<<"id">>, Params), #{}) of
        {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
            [Rule] = get_one_rule(AllRules, Id),
            {200, format_rule_resp(Rule)};
        {error, Reason} ->
            ?SLOG(error, #{msg => "update_rule_failed",
                           id => Id, reason => Reason}),
            {400, #{code => 'BAD_ARGS', message => ?ERR_BADARGS(Reason)}}
    end;

crud_rules_by_id(delete, #{bindings := #{id := Id}}) ->
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx:remove_config(ConfPath, #{}) of
        {ok, _} -> {204};
        {error, Reason} ->
            ?SLOG(error, #{msg => "delete_rule_failed",
                           id => Id, reason => Reason}),
            {500, #{code => 'BAD_ARGS', message => ?ERR_BADARGS(Reason)}}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
err_msg(Msg) ->
    list_to_binary(io_lib:format("~0p", [Msg])).


format_rule_resp(Rules) when is_list(Rules) ->
    [format_rule_resp(R) || R <- Rules];

format_rule_resp(#{ id := Id, created_at := CreatedAt,
                    from := Topics,
                    outputs := Output,
                    sql := SQL,
                    enabled := Enabled,
                    description := Descr}) ->
    #{id => Id,
      from => Topics,
      outputs => format_output(Output),
      sql => SQL,
      metrics => get_rule_metrics(Id),
      enabled => Enabled,
      created_at => format_datetime(CreatedAt, millisecond),
      description => Descr
     }.

format_datetime(Timestamp, Unit) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, Unit}])).

format_output(Outputs) ->
    [do_format_output(Out) || Out <- Outputs].

do_format_output(#{mod := Mod, func := Func, args := Args}) ->
    #{function => list_to_binary(lists:concat([Mod,":",Func])),
      args => maps:remove(preprocessed_tmpl, Args)};
do_format_output(BridgeChannelId) when is_binary(BridgeChannelId) ->
    BridgeChannelId.

get_rule_metrics(Id) ->
    Format = fun (Node, #{matched := Matched,
                          speed := Current,
                          speed_max := Max,
                          speed_last5m := Last5M
                        }) ->
        #{ matched => Matched
         , speed => Current
         , speed_max => Max
         , speed_last5m => Last5M
         , node => Node
         }
    end,
    [Format(Node, rpc:call(Node, emqx_plugin_libs_metrics, get_metrics, [rule_metrics, Id]))
     || Node <- mria_mnesia:running_nodes()].

get_one_rule(AllRules, Id) ->
    [R || R = #{id := Id0} <- AllRules, Id0 == Id].
