%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_cluster_link.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    api_spec/0,
    paths/0,
    namespace/0,
    fields/1,
    schema/1
]).

-export([
    '/cluster/links'/2,
    '/cluster/links/link/:name'/2,
    '/cluster/links/link/:name/metrics'/2,
    '/cluster/links/link/:name/metrics/reset'/2
]).

%% Request filters:
-export([filter_nonexistent/2]).

-export([scopes/0]).

-define(CONF_PATH, [cluster, links]).
-define(TAGS, [<<"Cluster">>]).

-type cluster_name() :: binary().

namespace() -> "cluster_link".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

scopes() -> ?SCOPE_CLUSTER_OPERATIONS.

paths() ->
    [
        "/cluster/links",
        "/cluster/links/link/:name",
        "/cluster/links/link/:name/metrics",
        "/cluster/links/link/:name/metrics/reset"
    ].

schema("/cluster/links") ->
    #{
        'operationId' => '/cluster/links',
        get =>
            #{
                description => ?DESC("get_cluster_links_config"),
                tags => ?TAGS,
                responses =>
                    #{200 => links_config_schema_response()}
            },
        post =>
            #{
                description => ?DESC("create_cluster_link"),
                tags => ?TAGS,
                'requestBody' => link_config_schema(),
                responses =>
                    #{
                        201 => link_config_schema_response(),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST, ?ALREADY_EXISTS],
                                ?DESC("update_config_failed")
                            )
                    }
            }
    };
schema("/cluster/links/link/:name") ->
    #{
        'operationId' => '/cluster/links/link/:name',
        filter => fun ?MODULE:filter_nonexistent/2,
        get =>
            #{
                description => ?DESC("get_cluster_link_config"),
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        200 => link_config_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC("cluster_link_not_found")
                        )
                    }
            },
        delete =>
            #{
                description => ?DESC("delete_cluster_link"),
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        204 => ?DESC("link_deleted"),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC("cluster_link_not_found")
                        )
                    }
            },
        put =>
            #{
                description => ?DESC("update_cluster_link_config"),
                tags => ?TAGS,
                parameters => [param_path_name()],
                'requestBody' => update_link_config_schema(),
                responses =>
                    #{
                        200 => link_config_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC("cluster_link_not_found")
                        ),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST], ?DESC("update_config_failed")
                            )
                    }
            }
    };
schema("/cluster/links/link/:name/metrics") ->
    #{
        'operationId' => '/cluster/links/link/:name/metrics',
        filter => fun ?MODULE:filter_nonexistent/2,
        get =>
            #{
                description => ?DESC("get_cluster_link_metrics"),
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        200 => link_metrics_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC("cluster_link_not_found")
                        )
                    }
            }
    };
schema("/cluster/links/link/:name/metrics/reset") ->
    #{
        'operationId' => '/cluster/links/link/:name/metrics/reset',
        filter => fun ?MODULE:filter_nonexistent/2,
        put =>
            #{
                description => ?DESC("reset_cluster_link_metrics"),
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        204 => ?DESC("reset"),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], ?DESC("cluster_link_not_found")
                        )
                    }
            }
    }.

fields(link_config_response) ->
    [
        {node, hoconsc:mk(binary(), #{desc => ?DESC("node")})},
        {status, hoconsc:mk(status(), #{desc => ?DESC("status")})}
        | emqx_cluster_link_schema:fields("link")
    ];
fields(metrics) ->
    [
        {metrics, hoconsc:mk(map(), #{desc => ?DESC("metrics")})}
    ];
fields(link_metrics_response) ->
    [
        {node_metrics,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(?MODULE, node_metrics)),
                #{desc => ?DESC("node_metrics")}
            )}
        | fields(metrics)
    ];
fields(node_metrics) ->
    [
        {node, hoconsc:mk(atom(), #{desc => ?DESC("node")})}
        | fields(metrics)
    ].

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

-define(LINK_NOT_FOUND, ?NOT_FOUND(<<"Cluster link not found">>)).
-define(LINK_ALREADY_EXISTS, ?BAD_REQUEST('ALREADY_EXISTS', <<"Cluster link already exists">>)).

'/cluster/links'(get, _Params) ->
    handle_list();
'/cluster/links'(post, #{body := Body = #{<<"name">> := Name}}) ->
    case emqx_cluster_link_config:get_link_raw(Name) of
        undefined ->
            handle_create(Name, Body);
        _Link = #{} ->
            ?LINK_ALREADY_EXISTS
    end.

'/cluster/links/link/:name'(get, #{bindings := #{name := Name}, link := Link}) ->
    handle_lookup(Name, Link);
'/cluster/links/link/:name'(put, #{bindings := #{name := Name}, link := Link, body := Update}) ->
    handle_update(Name, Update, Link);
'/cluster/links/link/:name'(delete, #{bindings := #{name := Name}}) ->
    handle_delete(Name).

'/cluster/links/link/:name/metrics'(get, #{bindings := #{name := Name}}) ->
    handle_metrics(Name).

'/cluster/links/link/:name/metrics/reset'(put, #{bindings := #{name := Name}}) ->
    handle_reset_metrics(Name).

filter_nonexistent(Request = #{bindings := Bindings}, _Meta) ->
    case Bindings of
        #{name := Name} ->
            case emqx_cluster_link_config:get_link_raw(Name) of
                Link = #{} ->
                    {ok, Request#{link => Link}};
                undefined ->
                    ?LINK_NOT_FOUND
            end;
        #{} ->
            ?LINK_NOT_FOUND
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

handle_list() ->
    Links = get_raw(),
    NodeRPCResults = emqx_cluster_link_mqtt:get_all_resources_cluster(),
    {NameToStatus, Errors} = collect_all_status(NodeRPCResults),
    NodeErrors = lists:map(
        fun({Node, Error}) ->
            #{node => Node, status => inconsistent, reason => Error}
        end,
        Errors
    ),
    InconsistentStatus = #{status => inconsistent, node_status => NodeErrors},
    DisabledStatus = #{status => ?status_disconnected, node_status => NodeErrors},
    Response =
        lists:map(
            fun(#{<<"name">> := Name} = Link) ->
                IsEnabled = maps:get(<<"enable">>, Link, true),
                Status =
                    case maps:find(Name, NameToStatus) of
                        error when IsEnabled ->
                            InconsistentStatus;
                        error ->
                            DisabledStatus;
                        {ok, Status0} ->
                            Status0
                    end,
                redact(maps:merge(Link, Status))
            end,
            Links
        ),
    ?OK(Response).

handle_create(Name, Params) ->
    Check =
        try
            ok = emqx_resource:validate_name(Name)
        catch
            throw:Error ->
                ?BAD_REQUEST(emqx_mgmt_api_lib:to_json(redact(Error)))
        end,
    case Check of
        ok ->
            case emqx_cluster_link_config:create_link(Params) of
                {ok, Link} ->
                    ?CREATED(redact(add_status(Name, Link)));
                {error, already_exists} ->
                    ?LINK_ALREADY_EXISTS;
                {error, single_node_license} ->
                    ?BAD_REQUEST(single_node_license_message());
                {error, Reason} ->
                    Message = emqx_utils:format("Create link failed: ~p", [redact(Reason)]),
                    ?BAD_REQUEST(Message)
            end;
        BadRequest ->
            redact(BadRequest)
    end.

handle_lookup(Name, Link0) ->
    Link1 = fill_defaults_single(Link0),
    Link = add_status(Name, Link1),
    ?OK(redact(Link)).

handle_update(Name, Params0, OldLinkRaw) ->
    Params1 = Params0#{<<"name">> => Name},
    Params = emqx_utils:deobfuscate(Params1, OldLinkRaw),
    case emqx_cluster_link_config:update_link(Params) of
        {ok, Link} ->
            ?OK(redact(add_status(Name, Link)));
        {error, not_found} ->
            ?LINK_NOT_FOUND;
        {error, single_node_license} ->
            ?BAD_REQUEST(single_node_license_message());
        {error, Reason} ->
            Message = emqx_utils:format("Update link failed: ~p", [redact(Reason)]),
            ?BAD_REQUEST(Message)
    end.

handle_delete(Name) ->
    case emqx_cluster_link_config:delete_link(Name) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?LINK_NOT_FOUND;
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Delete link failed: ~p", [Reason])),
            ?BAD_REQUEST(Message)
    end.

single_node_license_message() ->
    <<
        "Cluster linking is not available under the community (single-node) license. "
        "Load a non-community license and retry."
    >>.

handle_metrics(Name) ->
    Results = emqx_cluster_link_metrics:get_cluster_metrics(Name),
    {NodeMetrics0, NodeErrors} =
        lists:foldl(
            fun({Node, RouterMetrics0, ResourceMetrics0}, {OkAccIn, ErrAccIn}) ->
                {RouterMetrics, RouterError} = get_metrics_or_errors(RouterMetrics0),
                {ResourceMetrics, ResourceError} = get_metrics_or_errors(ResourceMetrics0),
                ErrAcc = append_errors(RouterError, ResourceError, Node, ErrAccIn),
                {[format_metrics(Node, RouterMetrics, ResourceMetrics) | OkAccIn], ErrAcc}
            end,
            {[], []},
            Results
        ),
    case NodeErrors of
        [] ->
            ok;
        [_ | _] ->
            ?SLOG(warning, #{
                msg => "cluster_link_api_metrics_bad_erpc_results",
                errors => maps:from_list(NodeErrors)
            })
    end,
    NodeMetrics1 = lists:map(fun({Node, _Error}) -> format_metrics(Node, #{}, #{}) end, NodeErrors),
    NodeMetrics = NodeMetrics1 ++ NodeMetrics0,
    AggregatedMetrics = aggregate_metrics(NodeMetrics),
    Response = #{metrics => AggregatedMetrics, node_metrics => NodeMetrics},
    ?OK(Response).

get_metrics_or_errors({ok, Metrics}) ->
    {Metrics, undefined};
get_metrics_or_errors(Error) ->
    {#{}, Error}.

append_errors(undefined, undefined, _Node, Acc) ->
    Acc;
append_errors(RouterError, ResourceError, Node, Acc) ->
    Err0 = emqx_utils_maps:put_if(#{}, router, RouterError, RouterError =/= undefined),
    Err = emqx_utils_maps:put_if(Err0, resource, ResourceError, ResourceError =/= undefined),
    [{Node, Err} | Acc].

aggregate_metrics(NodeMetrics) ->
    ErrorLogger = fun(_) -> ok end,
    #{metrics := #{router := EmptyRouterMetrics}} = format_metrics(node(), #{}, #{}),
    {RouterMetrics, ResourceMetrics} = lists:foldl(
        fun(
            #{metrics := #{router := RMetrics, forwarding := FMetrics}},
            {RouterAccIn, ResourceAccIn}
        ) ->
            ResourceAcc =
                emqx_utils_maps:best_effort_recursive_sum(FMetrics, ResourceAccIn, ErrorLogger),
            RouterAcc = merge_cluster_wide_metrics(RMetrics, RouterAccIn),
            {RouterAcc, ResourceAcc}
        end,
        {EmptyRouterMetrics, #{}},
        NodeMetrics
    ),
    #{router => RouterMetrics, forwarding => ResourceMetrics}.

merge_cluster_wide_metrics(Metrics, Acc) ->
    %% For cluster-wide metrics, all nodes should report the same values, except if the
    %% RPC to fetch a node's metrics failed, in which case all values will be 0.
    F =
        fun(_Key, V1, V2) ->
            case {erlang:is_map(V1), erlang:is_map(V2)} of
                {true, true} ->
                    merge_cluster_wide_metrics(V1, V2);
                {true, false} ->
                    merge_cluster_wide_metrics(V1, #{});
                {false, true} ->
                    merge_cluster_wide_metrics(V2, #{});
                {false, false} ->
                    true = is_number(V1),
                    true = is_number(V2),
                    max(V1, V2)
            end
        end,
    maps:merge_with(F, Acc, Metrics).

format_metrics(Node, RouterMetrics, ResourceMetrics) ->
    Get = fun(Path, Map) -> emqx_utils_maps:deep_get(Path, Map, 0) end,
    Routes = Get([gauges, ?route_metric], RouterMetrics),
    #{
        node => Node,
        metrics => #{
            router => #{
                ?route_metric => Routes
            },
            forwarding => #{
                'matched' => Get([counters, 'matched'], ResourceMetrics),
                'success' => Get([counters, 'success'], ResourceMetrics),
                'failed' => Get([counters, 'failed'], ResourceMetrics),
                'dropped' => Get([counters, 'dropped'], ResourceMetrics),
                'retried' => Get([counters, 'retried'], ResourceMetrics),

                'queuing' => Get([gauges, 'queuing'], ResourceMetrics),
                'inflight' => Get([gauges, 'inflight'], ResourceMetrics),

                'rate' => Get([rate, 'matched', current], ResourceMetrics),
                'rate_last5m' => Get([rate, 'matched', last5m], ResourceMetrics),
                'rate_max' => Get([rate, 'matched', max], ResourceMetrics)
            }
        }
    }.

handle_reset_metrics(Name) ->
    Res = emqx_cluster_link_metrics:reset_cluster_metrics(Name),
    ErrorNodes =
        lists:filtermap(
            fun
                ({_Node, {ok, ok}, {ok, ok}}) ->
                    false;
                ({Node, _, _}) ->
                    {true, Node}
            end,
            Res
        ),
    case ErrorNodes of
        [] ->
            ?NO_CONTENT;
        [_ | _] ->
            Msg0 = <<"Metrics reset failed on one or more nodes. Please try again.">>,
            Msg1 = ?ERROR_MSG('INTERNAL_ERROR', Msg0),
            Msg = Msg1#{nodes => ErrorNodes},
            {500, Msg}
    end.

add_status(Name, Link) ->
    NodeRPCResults = emqx_cluster_link_mqtt:get_resource_cluster(Name),
    Status = collect_single_status(NodeRPCResults),
    maps:merge(Link, Status).

get_raw() ->
    #{<<"cluster">> := #{<<"links">> := Links}} =
        emqx_config:fill_defaults(
            #{<<"cluster">> => #{<<"links">> => emqx_conf:get_raw(?CONF_PATH)}},
            #{obfuscate_sensitive_values => true}
        ),
    Links.

-spec collect_all_status([{node(), {ok, #{cluster_name() => _}} | _Error}]) ->
    {ClusterToStatus, Errors}
when
    ClusterToStatus :: #{
        cluster_name() => #{
            node := node(),
            status := emqx_resource:resource_status() | inconsistent
        }
    },
    Errors :: [{node(), term()}].
collect_all_status(NodeResults) ->
    {Reindexed, Errors} = lists:foldl(
        fun
            ({Node, {ok, AllLinkData}}, {OkAccIn, ErrAccIn}) ->
                OkAcc = maps:fold(
                    fun(Name, Data, AccIn) ->
                        collect_all_status1(Node, Name, Data, AccIn)
                    end,
                    OkAccIn,
                    AllLinkData
                ),
                {OkAcc, ErrAccIn};
            ({Node, Error}, {OkAccIn, ErrAccIn}) ->
                {OkAccIn, [{Node, Error} | ErrAccIn]}
        end,
        {#{}, []},
        NodeResults
    ),
    NoErrors =
        case Errors of
            [] ->
                true;
            [_ | _] ->
                ?SLOG(warning, #{
                    msg => "cluster_link_api_lookup_status_bad_erpc_results",
                    errors => Errors
                }),
                false
        end,
    ClusterToStatus = maps:fold(
        fun(Name, NodeToData, Acc) ->
            OnlyStatus = [S || #{status := S} <- maps:values(NodeToData)],
            SummaryStatus =
                case lists:usort(OnlyStatus) of
                    [SameStatus] when NoErrors -> SameStatus;
                    _ -> inconsistent
                end,
            NodeStatus = lists:map(
                fun
                    ({Node, #{status := S}}) ->
                        #{node => Node, status => S};
                    ({Node, Error0}) ->
                        Error = emqx_utils_json:best_effort_json_obj(Error0),
                        #{node => Node, status => inconsistent, reason => Error}
                end,
                maps:to_list(NodeToData) ++ Errors
            ),
            Acc#{
                Name => #{
                    status => SummaryStatus,
                    node_status => NodeStatus
                }
            }
        end,
        #{},
        Reindexed
    ),
    {ClusterToStatus, Errors}.

collect_all_status1(Node, Name, Data, Acc) ->
    maps:update_with(
        Name,
        fun(Old) -> Old#{Node => Data} end,
        #{Node => Data},
        Acc
    ).

collect_single_status(NodeResults) ->
    NodeStatus =
        lists:map(
            fun
                ({Node, {ok, {ok, #{status := S}}}}) ->
                    #{node => Node, status => S};
                ({Node, {ok, {error, _}}}) ->
                    #{node => Node, status => ?status_disconnected};
                ({Node, Error0}) ->
                    Error = emqx_utils_json:best_effort_json_obj(Error0),
                    #{node => Node, status => inconsistent, reason => Error}
            end,
            NodeResults
        ),
    OnlyStatus = [S || #{status := S} <- NodeStatus],
    SummaryStatus =
        case lists:usort(OnlyStatus) of
            [SameStatus] -> SameStatus;
            _ -> inconsistent
        end,
    #{
        status => SummaryStatus,
        node_status => NodeStatus
    }.

links_config_schema_response() ->
    hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, link_config_response)), #{
        examples => #{<<"example">> => links_config_response_example()}
    }).

link_config_schema() ->
    hoconsc:mk(emqx_cluster_link_schema:link_schema(), #{
        examples => #{<<"example">> => hd(links_config_example())}
    }).

link_config_schema_response() ->
    hoconsc:mk(
        hoconsc:ref(?MODULE, link_config_response),
        #{
            examples => #{
                <<"example">> => hd(links_config_response_example())
            }
        }
    ).

link_metrics_schema_response() ->
    hoconsc:mk(
        hoconsc:ref(?MODULE, link_metrics_response),
        #{
            examples => #{
                <<"example">> => link_metrics_response_example()
            }
        }
    ).

status() ->
    hoconsc:enum([?status_connected, ?status_disconnected, ?status_connecting, inconsistent]).

param_path_name() ->
    {name,
        hoconsc:mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_link">>,
                desc => ?DESC("param_path_name")
            }
        )}.

update_link_config_schema() ->
    proplists:delete(name, emqx_cluster_link_schema:fields("link")).

links_config_response_example() ->
    lists:map(
        fun(LinkEx) ->
            LinkEx#{
                <<"status">> => <<"connected">>,
                <<"node_status">> => [
                    #{
                        <<"node">> => <<"emqx1@emqx.net">>,
                        <<"status">> => <<"connected">>
                    }
                ]
            }
        end,
        links_config_example()
    ).

links_config_example() ->
    [
        #{
            <<"name">> => <<"emqxcl_b">>,
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"resource_opts">> => #{
                <<"dispatch_strategy">> => <<"per_clientid">>,
                <<"health_check_interval">> => <<"15s">>,
                <<"request_ttl">> => <<"45s">>,
                <<"worker_pool_size">> => 16
            },
            <<"server">> => <<"emqxcl_b.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> => [
                <<"t/topic-example">>,
                <<"t/topic-filter-example/1/#">>
            ]
        },
        #{
            <<"name">> => <<"emqxcl_c">>,
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"resource_opts">> => #{
                <<"dispatch_strategy">> => <<"random">>,
                <<"health_check_interval">> => <<"10s">>,
                <<"request_ttl">> => <<"30s">>,
                <<"worker_pool_size">> => 16
            },
            <<"server">> => <<"emqxcl_c.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> => [
                <<"t/topic-example">>,
                <<"t/topic-filter-example/1/#">>
            ]
        }
    ].

link_metrics_response_example() ->
    #{
        <<"metrics">> => #{<<"routes">> => 10240},
        <<"node_metrics">> => [
            #{
                <<"node">> => <<"emqx1@emqx.net">>,
                <<"metrics">> => #{<<"routes">> => 10240}
            }
        ]
    }.

fill_defaults_single(Link0) ->
    #{<<"cluster">> := #{<<"links">> := [Link]}} =
        emqx_config:fill_defaults(
            #{<<"cluster">> => #{<<"links">> => [Link0]}},
            #{obfuscate_sensitive_values => false}
        ),
    Link.

redact(Value) ->
    emqx_utils:redact(Value).
