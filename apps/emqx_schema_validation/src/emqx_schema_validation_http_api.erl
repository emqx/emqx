%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_http_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    fields/1,
    paths/0,
    schema/1
]).

%% `minirest' handlers
-export([
    '/schema_validations'/2,
    '/schema_validations/reorder'/2,
    '/schema_validations/validation/:name'/2,
    '/schema_validations/validation/:name/metrics'/2,
    '/schema_validations/validation/:name/metrics/reset'/2,
    '/schema_validations/validation/:name/enable/:enable'/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"Schema Validation">>]).
-define(METRIC_NAME, schema_validation).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "schema_validation_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/schema_validations",
        "/schema_validations/reorder",
        "/schema_validations/validation/:name",
        "/schema_validations/validation/:name/metrics",
        "/schema_validations/validation/:name/metrics/reset",
        "/schema_validations/validation/:name/enable/:enable"
    ].

schema("/schema_validations") ->
    #{
        'operationId' => '/schema_validations',
        get => #{
            tags => ?TAGS,
            summary => <<"List validations">>,
            description => ?DESC("list_validations"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(
                                emqx_schema_validation_schema:api_schema(list)
                            ),
                            example_return_list()
                        )
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Append a new validation">>,
            description => ?DESC("append_validation"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_schema_validation_schema:api_schema(post),
                example_input_create()
            ),
            responses =>
                #{
                    201 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_validation_schema:api_schema(post),
                            example_return_create()
                        ),
                    400 => error_schema('ALREADY_EXISTS', "Validation already exists")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update a validation">>,
            description => ?DESC("update_validation"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_schema_validation_schema:api_schema(put),
                example_input_update()
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_validation_schema:api_schema(put),
                            example_return_update()
                        ),
                    404 => error_schema('NOT_FOUND', "Validation not found"),
                    400 => error_schema('BAD_REQUEST', "Bad params")
                }
        }
    };
schema("/schema_validations/reorder") ->
    #{
        'operationId' => '/schema_validations/reorder',
        post => #{
            tags => ?TAGS,
            summary => <<"Reorder all validations">>,
            description => ?DESC("reorder_validations"),
            'requestBody' =>
                emqx_dashboard_swagger:schema_with_examples(
                    ref(reorder),
                    example_input_reorder()
                ),
            responses =>
                #{
                    204 => <<"No Content">>,
                    400 => error_schema(
                        'BAD_REQUEST',
                        <<"Bad request">>,
                        [
                            {not_found, mk(array(binary()), #{desc => "Validations not found"})},
                            {not_reordered,
                                mk(array(binary()), #{desc => "Validations not referenced in input"})},
                            {duplicated,
                                mk(array(binary()), #{desc => "Duplicated validations in input"})}
                        ]
                    )
                }
        }
    };
schema("/schema_validations/validation/:name") ->
    #{
        'operationId' => '/schema_validations/validation/:name',
        get => #{
            tags => ?TAGS,
            summary => <<"Lookup a validation">>,
            description => ?DESC("lookup_validation"),
            parameters => [param_path_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(
                                emqx_schema_validation_schema:api_schema(lookup)
                            ),
                            example_return_lookup()
                        ),
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete a validation">>,
            description => ?DESC("delete_validation"),
            parameters => [param_path_name()],
            responses =>
                #{
                    204 => <<"Validation deleted">>,
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        }
    };
schema("/schema_validations/validation/:name/metrics") ->
    #{
        'operationId' => '/schema_validations/validation/:name/metrics',
        get => #{
            tags => ?TAGS,
            summary => <<"Get validation metrics">>,
            description => ?DESC("get_validation_metrics"),
            parameters => [param_path_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ref(get_metrics),
                            example_return_metrics()
                        ),
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        }
    };
schema("/schema_validations/validation/:name/metrics/reset") ->
    #{
        'operationId' => '/schema_validations/validation/:name/metrics/reset',
        post => #{
            tags => ?TAGS,
            summary => <<"Reset validation metrics">>,
            description => ?DESC("reset_validation_metrics"),
            parameters => [param_path_name()],
            responses =>
                #{
                    204 => <<"No content">>,
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        }
    };
schema("/schema_validations/validation/:name/enable/:enable") ->
    #{
        'operationId' => '/schema_validations/validation/:name/enable/:enable',
        post => #{
            tags => ?TAGS,
            summary => <<"Enable or disable validation">>,
            description => ?DESC("enable_disable_validation"),
            parameters => [param_path_name(), param_path_enable()],
            responses =>
                #{
                    204 => <<"No content">>,
                    404 => error_schema('NOT_FOUND', "Validation not found"),
                    400 => error_schema('BAD_REQUEST', "Bad params")
                }
        }
    }.

param_path_name() ->
    {name,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_validation">>,
                desc => ?DESC("param_path_name")
            }
        )}.

param_path_enable() ->
    {enable,
        mk(
            boolean(),
            #{
                in => path,
                required => true,
                desc => ?DESC("param_path_enable")
            }
        )}.

fields(front) ->
    [{position, mk(front, #{default => front, required => true, in => body})}];
fields(rear) ->
    [{position, mk(rear, #{default => rear, required => true, in => body})}];
fields('after') ->
    [
        {position, mk('after', #{default => 'after', required => true, in => body})},
        {validation, mk(binary(), #{required => true, in => body})}
    ];
fields(before) ->
    [
        {position, mk(before, #{default => before, required => true, in => body})},
        {validation, mk(binary(), #{required => true, in => body})}
    ];
fields(reorder) ->
    [
        {order, mk(array(binary()), #{required => true, in => body})}
    ];
fields(get_metrics) ->
    [
        {metrics, mk(ref(metrics), #{})},
        {node_metrics, mk(ref(node_metrics), #{})}
    ];
fields(metrics) ->
    [
        {matched, mk(non_neg_integer(), #{})},
        {succeeded, mk(non_neg_integer(), #{})},
        {failed, mk(non_neg_integer(), #{})}
    ];
fields(node_metrics) ->
    [
        {node, mk(binary(), #{})}
        | fields(metrics)
    ].

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

'/schema_validations'(get, _Params) ->
    ?OK(emqx_schema_validation:list());
'/schema_validations'(post, #{body := Params = #{<<"name">> := Name}}) ->
    with_validation(
        Name,
        return(?BAD_REQUEST('ALREADY_EXISTS', <<"Validation already exists">>)),
        fun() ->
            case emqx_schema_validation:insert(Params) of
                {ok, _} ->
                    {ok, Res} = emqx_schema_validation:lookup(Name),
                    {201, Res};
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end
    );
'/schema_validations'(put, #{body := Params = #{<<"name">> := Name}}) ->
    with_validation(
        Name,
        fun() ->
            case emqx_schema_validation:update(Params) of
                {ok, _} ->
                    {ok, Res} = emqx_schema_validation:lookup(Name),
                    {200, Res};
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        not_found()
    ).

'/schema_validations/validation/:name'(get, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun(Validation) -> ?OK(Validation) end,
        not_found()
    );
'/schema_validations/validation/:name'(delete, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun() ->
            case emqx_schema_validation:delete(Name) of
                {ok, _} ->
                    ?NO_CONTENT;
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        not_found()
    ).

'/schema_validations/reorder'(post, #{body := #{<<"order">> := Order}}) ->
    do_reorder(Order).

'/schema_validations/validation/:name/enable/:enable'(post, #{
    bindings := #{name := Name, enable := Enable}
}) ->
    with_validation(
        Name,
        fun(Validation) -> do_enable_disable(Validation, Enable) end,
        not_found()
    ).

'/schema_validations/validation/:name/metrics'(get, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun() ->
            Nodes = emqx:running_nodes(),
            Results = emqx_metrics_proto_v2:get_metrics(Nodes, ?METRIC_NAME, Name, 5_000),
            NodeResults = lists:zip(Nodes, Results),
            NodeErrors = [Result || Result = {_Node, {NOk, _}} <- NodeResults, NOk =/= ok],
            NodeErrors == [] orelse
                ?SLOG(warning, #{
                    msg => "rpc_get_validation_metrics_errors",
                    errors => NodeErrors
                }),
            NodeMetrics = [format_metrics(Node, Metrics) || {Node, {ok, Metrics}} <- NodeResults],
            Response = #{
                metrics => aggregate_metrics(NodeMetrics),
                node_metrics => NodeMetrics
            },
            ?OK(Response)
        end,
        not_found()
    ).

'/schema_validations/validation/:name/metrics/reset'(post, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun() ->
            Nodes = emqx:running_nodes(),
            Results = emqx_metrics_proto_v2:reset_metrics(Nodes, ?METRIC_NAME, Name, 5_000),
            NodeResults = lists:zip(Nodes, Results),
            NodeErrors = [Result || Result = {_Node, {NOk, _}} <- NodeResults, NOk =/= ok],
            NodeErrors == [] orelse
                ?SLOG(warning, #{
                    msg => "rpc_reset_validation_metrics_errors",
                    errors => NodeErrors
                }),
            ?NO_CONTENT
        end,
        not_found()
    ).

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

ref(Struct) -> hoconsc:ref(?MODULE, Struct).
mk(Type, Opts) -> hoconsc:mk(Type, Opts).
array(Type) -> hoconsc:array(Type).

example_input_create() ->
    #{
        <<"sql_check">> =>
            #{
                summary => <<"Using a SQL check">>,
                value => example_validation([example_sql_check()])
            },
        <<"avro_check">> =>
            #{
                summary => <<"Using an Avro schema check">>,
                value => example_validation([example_avro_check()])
            }
    }.

example_input_update() ->
    #{
        <<"update">> =>
            #{
                summary => <<"Update">>,
                value => example_validation([example_sql_check()])
            }
    }.

example_input_reorder() ->
    #{
        <<"reorder">> =>
            #{
                summary => <<"Update">>,
                value => #{
                    order => [<<"bar">>, <<"foo">>, <<"baz">>]
                }
            }
    }.

example_return_list() ->
    OtherVal0 = example_validation([example_avro_check()]),
    OtherVal = OtherVal0#{name => <<"other_validation">>},
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [
                    example_validation([example_sql_check()]),
                    OtherVal
                ]
            }
    }.

example_return_create() ->
    example_input_create().

example_return_update() ->
    example_input_update().

example_return_lookup() ->
    example_input_create().

example_return_metrics() ->
    Metrics = #{
        matched => 2,
        succeeded => 1,
        failed => 1,
        rate => 1.23,
        rate_last5m => 0.88,
        rate_max => 1.87
    },
    #{
        <<"metrics">> =>
            #{
                summary => <<"Metrics">>,
                value => #{
                    metrics => Metrics,
                    node_metrics =>
                        [
                            #{
                                node => <<"emqx@127.0.0.1">>,
                                metrics => Metrics
                            }
                        ]
                }
            }
    }.

example_validation(Checks) ->
    #{
        name => <<"my_validation">>,
        enable => true,
        description => <<"my validation">>,
        tags => [<<"validation">>],
        topics => [<<"t/+">>],
        strategy => <<"all_pass">>,
        failure_action => <<"drop">>,
        log_failure => #{<<"level">> => <<"info">>},
        checks => Checks
    }.

example_sql_check() ->
    #{
        type => <<"sql">>,
        sql => <<"select payload.temp as t where t > 10">>
    }.

example_avro_check() ->
    #{
        type => <<"avro">>,
        schema => <<"my_avro_schema">>
    }.

error_schema(Code, Message) ->
    error_schema(Code, Message, _ExtraFields = []).

error_schema(Code, Message, ExtraFields) when is_atom(Code) ->
    error_schema([Code], Message, ExtraFields);
error_schema(Codes, Message, ExtraFields) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message), ExtraFields);
error_schema(Codes, Message, ExtraFields) when is_list(Codes) andalso is_binary(Message) ->
    ExtraFields ++ emqx_dashboard_swagger:error_codes(Codes, Message).

do_reorder(Order) ->
    case emqx_schema_validation:reorder(Order) of
        {ok, _} ->
            ?NO_CONTENT;
        {error,
            {pre_config_update, _HandlerMod, #{
                not_found := NotFound,
                duplicated := Duplicated,
                not_reordered := NotReordered
            }}} ->
            Msg0 = ?ERROR_MSG('BAD_REQUEST', <<"Bad request">>),
            Msg = Msg0#{
                not_found => NotFound,
                duplicated => Duplicated,
                not_reordered => NotReordered
            },
            {400, Msg};
        {error, Error} ->
            ?BAD_REQUEST(Error)
    end.

do_enable_disable(Validation, Enable) ->
    RawValidation = make_serializable(Validation),
    case emqx_schema_validation:update(RawValidation#{<<"enable">> => Enable}) of
        {ok, _} ->
            ?NO_CONTENT;
        {error, Reason} ->
            ?BAD_REQUEST(Reason)
    end.

with_validation(Name, FoundFn, NotFoundFn) ->
    case emqx_schema_validation:lookup(Name) of
        {ok, Validation} ->
            {arity, Arity} = erlang:fun_info(FoundFn, arity),
            case Arity of
                1 -> FoundFn(Validation);
                0 -> FoundFn()
            end;
        {error, not_found} ->
            NotFoundFn()
    end.

return(Response) ->
    fun() -> Response end.

not_found() ->
    return(?NOT_FOUND(<<"Validation not found">>)).

make_serializable(Validation) ->
    Schema = emqx_schema_validation_schema,
    RawConfig = #{
        <<"schema_validation">> => #{
            <<"validations">> =>
                [emqx_utils_maps:binary_key_map(Validation)]
        }
    },
    #{
        <<"schema_validation">> := #{
            <<"validations">> :=
                [Serialized]
        }
    } =
        hocon_tconf:make_serializable(Schema, RawConfig, #{}),
    Serialized.

format_metrics(Node, #{
    counters := #{
        'matched' := Matched,
        'succeeded' := Succeeded,
        'failed' := Failed
    },
    rate := #{
        'matched' := #{
            current := MatchedRate,
            last5m := Matched5mRate,
            max := MatchedMaxRate
        }
    }
}) ->
    #{
        metrics => #{
            'matched' => Matched,
            'succeeded' => Succeeded,
            'failed' => Failed,
            rate => MatchedRate,
            rate_last5m => Matched5mRate,
            rate_max => MatchedMaxRate
        },
        node => Node
    };
format_metrics(Node, _) ->
    #{
        metrics => #{
            'matched' => 0,
            'succeeded' => 0,
            'failed' => 0,
            rate => 0,
            rate_last5m => 0,
            rate_max => 0
        },
        node => Node
    }.

aggregate_metrics(NodeMetrics) ->
    ErrorLogger = fun(_) -> ok end,
    lists:foldl(
        fun(#{metrics := Metrics}, Acc) ->
            emqx_utils_maps:best_effort_recursive_sum(Metrics, Acc, ErrorLogger)
        end,
        #{},
        NodeMetrics
    ).
