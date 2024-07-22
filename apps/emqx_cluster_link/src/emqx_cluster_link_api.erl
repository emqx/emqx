%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/http_api.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_cluster_link.hrl").

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
    '/cluster/links/link/:name/metrics'/2
]).

-define(CONF_PATH, [cluster, links]).
-define(TAGS, [<<"Cluster">>]).

-type cluster_name() :: binary().

namespace() -> "cluster_link".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/cluster/links",
        "/cluster/links/link/:name",
        "/cluster/links/link/:name/metrics"
    ].

schema("/cluster/links") ->
    #{
        'operationId' => '/cluster/links',
        get =>
            #{
                description => "Get cluster links configuration",
                tags => ?TAGS,
                responses =>
                    #{200 => links_config_schema_response()}
            },
        post =>
            #{
                description => "Create a cluster link configuration",
                tags => ?TAGS,
                'requestBody' => link_config_schema(),
                responses =>
                    #{
                        200 => link_config_schema_response(),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST, ?ALREADY_EXISTS],
                                <<"Update Config Failed">>
                            )
                    }
            }
    };
schema("/cluster/links/link/:name") ->
    #{
        'operationId' => '/cluster/links/link/:name',
        get =>
            #{
                description => "Get a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        200 => link_config_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        )
                    }
            },
        delete =>
            #{
                description => "Delete a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        204 => <<"Link deleted">>,
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        )
                    }
            },
        put =>
            #{
                description => "Update a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                'requestBody' => update_link_config_schema(),
                responses =>
                    #{
                        200 => link_config_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        ),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST], <<"Update Config Failed">>
                            )
                    }
            }
    };
schema("/cluster/links/link/:name/metrics") ->
    #{
        'operationId' => '/cluster/links/link/:name/metrics',
        get =>
            #{
                description => "Get a cluster link metrics",
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        200 => link_metrics_schema_response(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
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

'/cluster/links'(get, _Params) ->
    handle_list();
'/cluster/links'(post, #{body := Body = #{<<"name">> := Name}}) ->
    with_link(
        Name,
        return(?BAD_REQUEST('ALREADY_EXISTS', <<"Cluster link already exists">>)),
        fun() -> handle_create(Name, Body) end
    ).

'/cluster/links/link/:name'(get, #{bindings := #{name := Name}}) ->
    with_link(Name, fun(Link) -> handle_lookup(Name, Link) end, not_found());
'/cluster/links/link/:name'(put, #{bindings := #{name := Name}, body := Params0}) ->
    with_link(Name, fun() -> handle_update(Name, Params0) end, not_found());
'/cluster/links/link/:name'(delete, #{bindings := #{name := Name}}) ->
    with_link(
        Name,
        fun() ->
            case emqx_cluster_link_config:delete(Name) of
                ok ->
                    ?NO_CONTENT;
                {error, Reason} ->
                    Message = list_to_binary(io_lib:format("Delete link failed ~p", [Reason])),
                    ?BAD_REQUEST(Message)
            end
        end,
        not_found()
    ).

'/cluster/links/link/:name/metrics'(get, #{bindings := #{name := Name}}) ->
    with_link(Name, fun() -> handle_metrics(Name) end, not_found()).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

handle_list() ->
    Links = get_raw(),
    NodeResults = get_all_link_status_cluster(),
    NameToStatus = collect_all_status(NodeResults),
    EmptyStatus = #{status => inconsistent, node_status => []},
    Response =
        lists:map(
            fun(#{<<"name">> := Name} = Link) ->
                Status = maps:get(Name, NameToStatus, EmptyStatus),
                maps:merge(Link, Status)
            end,
            Links
        ),
    ?OK(Response).

handle_create(Name, Params) ->
    case emqx_cluster_link_config:create(Params) of
        {ok, Link} ->
            ?CREATED(add_status(Name, Link));
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Create link failed ~p", [Reason])),
            ?BAD_REQUEST(Message)
    end.

handle_lookup(Name, Link) ->
    ?OK(add_status(Name, Link)).

handle_metrics(Name) ->
    case emqx_cluster_link:get_metrics(Name) of
        {error, BadResults} ->
            ?SLOG(warning, #{
                msg => "cluster_link_api_metrics_bad_erpc_results",
                results => BadResults
            }),
            ?OK(#{metrics => #{}, node_metrics => []});
        {ok, NodeResults} ->
            NodeMetrics =
                lists:map(
                    fun({Node, Metrics}) ->
                        format_metrics(Node, Metrics)
                    end,
                    NodeResults
                ),
            AggregatedMetrics = aggregate_metrics(NodeMetrics),
            Response = #{metrics => AggregatedMetrics, node_metrics => NodeMetrics},
            ?OK(Response)
    end.

aggregate_metrics(NodeMetrics) ->
    ErrorLogger = fun(_) -> ok end,
    lists:foldl(
        fun(#{metrics := Metrics}, Acc) ->
            emqx_utils_maps:best_effort_recursive_sum(Metrics, Acc, ErrorLogger)
        end,
        #{},
        NodeMetrics
    ).

format_metrics(Node, Metrics) ->
    Routes = emqx_utils_maps:deep_get([counters, ?route_metric], Metrics, 0),
    #{
        node => Node,
        metrics => #{
            ?route_metric => Routes
        }
    }.

add_status(Name, Link) ->
    NodeResults = get_link_status_cluster(Name),
    Status = collect_single_status(NodeResults),
    maps:merge(Link, Status).

handle_update(Name, Params0) ->
    Params = Params0#{<<"name">> => Name},
    case emqx_cluster_link_config:update_one_link(Params) of
        {ok, Link} ->
            ?OK(add_status(Name, Link));
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update link failed ~p", [Reason])),
            ?BAD_REQUEST(Message)
    end.

get_raw() ->
    #{<<"cluster">> := #{<<"links">> := Links}} =
        emqx_config:fill_defaults(
            #{<<"cluster">> => #{<<"links">> => emqx_conf:get_raw(?CONF_PATH)}},
            #{obfuscate_sensitive_values => true}
        ),
    Links.

get_all_link_status_cluster() ->
    case emqx_cluster_link_mqtt:get_all_resources_cluster() of
        {error, BadResults} ->
            ?SLOG(warning, #{
                msg => "cluster_link_api_all_status_bad_erpc_results",
                results => BadResults
            }),
            [];
        {ok, NodeResults} ->
            NodeResults
    end.

get_link_status_cluster(Name) ->
    case emqx_cluster_link_mqtt:get_resource_cluster(Name) of
        {error, BadResults} ->
            ?SLOG(warning, #{
                msg => "cluster_link_api_lookup_status_bad_erpc_results",
                results => BadResults
            }),
            [];
        {ok, NodeResults} ->
            NodeResults
    end.

-spec collect_all_status([{node(), #{cluster_name() => _}}]) ->
    #{
        cluster_name() => #{
            node := node(),
            status := emqx_resource:resource_status() | inconsistent
        }
    }.
collect_all_status(NodeResults) ->
    Reindexed = lists:foldl(
        fun({Node, AllLinkData}, Acc) ->
            maps:fold(
                fun(Name, Data, AccIn) ->
                    collect_all_status1(Node, Name, Data, AccIn)
                end,
                Acc,
                AllLinkData
            )
        end,
        #{},
        NodeResults
    ),
    maps:fold(
        fun(Name, NodeToData, Acc) ->
            OnlyStatus = [S || #{status := S} <- maps:values(NodeToData)],
            SummaryStatus =
                case lists:usort(OnlyStatus) of
                    [SameStatus] -> SameStatus;
                    _ -> inconsistent
                end,
            NodeStatus = lists:map(
                fun({Node, #{status := S}}) ->
                    #{node => Node, status => S}
                end,
                maps:to_list(NodeToData)
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
    ).

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
                ({Node, {ok, #{status := S}}}) ->
                    #{node => Node, status => S};
                ({Node, {error, _}}) ->
                    #{node => Node, status => ?status_disconnected};
                ({Node, _}) ->
                    #{node => Node, status => inconsistent}
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
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"server">> => <<"emqxcl_b.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> =>
                [
                    <<"t/topic-example">>,
                    <<"t/topic-filter-example/1/#">>
                ],
            <<"name">> => <<"emqxcl_b">>
        },
        #{
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"server">> => <<"emqxcl_c.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> =>
                [
                    <<"t/topic-example">>,
                    <<"t/topic-filter-example/1/#">>
                ],
            <<"name">> => <<"emqxcl_c">>
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

with_link(Name, FoundFn, NotFoundFn) ->
    case emqx_cluster_link_config:link_raw(Name) of
        undefined ->
            NotFoundFn();
        Link0 = #{} ->
            Link = fill_defaults_single(Link0),
            {arity, Arity} = erlang:fun_info(FoundFn, arity),
            case Arity of
                1 -> FoundFn(Link);
                0 -> FoundFn()
            end
    end.

fill_defaults_single(Link0) ->
    #{<<"cluster">> := #{<<"links">> := [Link]}} =
        emqx_config:fill_defaults(
            #{<<"cluster">> => #{<<"links">> => [Link0]}},
            #{obfuscate_sensitive_values => true}
        ),
    Link.

return(Response) ->
    fun() -> Response end.

not_found() ->
    return(?NOT_FOUND(<<"Cluster link not found">>)).
