%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_stats).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(
    hoconsc,
    [
        mk/2,
        ref/1,
        ref/2,
        array/1
    ]
).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([list/2]).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/stats"].

schema("/stats") ->
    #{
        'operationId' => list,
        get =>
            #{
                description => ?DESC(emqx_stats),
                tags => [<<"Metrics">>],
                parameters => [ref(aggregate)],
                responses =>
                    #{
                        200 => mk(
                            hoconsc:union([
                                array(ref(?MODULE, per_node_data)),
                                ref(?MODULE, aggregated_data)
                            ]),
                            #{desc => ?DESC("api_rsp_200")}
                        )
                    }
            }
    }.

fields(aggregate) ->
    [
        {aggregate,
            mk(
                boolean(),
                #{
                    desc => ?DESC("aggregate"),
                    in => query,
                    required => false,
                    example => false
                }
            )}
    ];
fields(aggregated_data) ->
    [
        stats_schema('channels.count', "channels_count"),
        stats_schema('channels.max', "channels_max"),
        stats_schema('connections.count', "connections_count"),
        stats_schema('connections.max', "connections_max"),
        stats_schema('delayed.count', "delayed_count"),
        stats_schema('delayed.max', "delayed_max"),
        stats_schema('live_connections.count', "live_connections_count"),
        stats_schema('live_connections.max', "live_connections_max"),
        stats_schema('cluster_sessions.count', "cluster_sessions_count"),
        stats_schema('cluster_sessions.max', "cluster_sessions_max"),
        stats_schema('retained.count', "retained_count"),
        stats_schema('retained.max', "retained_max"),
        stats_schema('sessions.count', "sessions_count"),
        stats_schema('sessions.max', "sessions_max"),
        stats_schema('suboptions.count', "suboptions_count"),
        stats_schema('suboptions.max', "suboptions_max"),
        stats_schema('subscribers.count', "subscribers_count"),
        stats_schema('subscribers.max', "subscribers_max"),
        stats_schema('subscriptions.count', "subscriptions_count"),
        stats_schema('subscriptions.max', "subscriptions_max"),
        stats_schema('subscriptions.shared.count', "subscriptions_shared_count"),
        stats_schema('subscriptions.shared.max', "subscriptions_shared_max"),
        stats_schema('topics.count', "topics_count"),
        stats_schema('topics.max', "topics_max")
    ];
fields(per_node_data) ->
    [
        {node,
            mk(string(), #{
                desc => ?DESC("node_name"),
                example => <<"emqx@127.0.0.1">>
            })},
        stats_schema('durable_subscriptions.count', "durable_subscriptions_count")
    ] ++ fields(aggregated_data).

stats_schema(Name, DescKey) ->
    {Name, mk(non_neg_integer(), #{desc => ?DESC(DescKey), example => 0})}.

%%%==============================================================================================
%% api apply
list(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, undefined) of
        true ->
            {200, emqx_mgmt:get_stats()};
        _ ->
            Data = lists:foldl(
                fun(Node, Acc) ->
                    case emqx_mgmt:get_stats(Node) of
                        {error, _Err} ->
                            Acc;
                        Stats when is_list(Stats) ->
                            Data = maps:from_list([{node, Node} | Stats]),
                            [Data | Acc]
                    end
                end,
                [],
                emqx:running_nodes()
            ),
            {200, Data}
    end.

%%%==============================================================================================
%% Internal
