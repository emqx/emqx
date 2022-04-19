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
-module(emqx_mgmt_api_stats).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").

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
    fields/1
]).

-export([list/2]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/stats"].

schema("/stats") ->
    #{
        'operationId' => list,
        get =>
            #{
                description => <<"EMQX stats">>,
                tags => [<<"stats">>],
                parameters => [ref(aggregate)],
                responses =>
                    #{
                        200 => mk(
                            hoconsc:union([
                                ref(?MODULE, node_stats_data),
                                array(ref(?MODULE, aggergate_data))
                            ]),
                            #{desc => <<"List stats ok">>}
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
                    desc => <<"Calculation aggregate for all nodes">>,
                    in => query,
                    required => false,
                    example => false
                }
            )}
    ];
fields(node_stats_data) ->
    [
        stats_schema('channels.count', <<"sessions.count">>),
        stats_schema('channels.max', <<"session.max">>),
        stats_schema('connections.count', <<"Number of current connections">>),
        stats_schema('connections.max', <<"Historical maximum number of connections">>),
        stats_schema('delayed.count', <<"Number of delayed messages">>),
        stats_schema('delayed.max', <<"Historical maximum number of delayed messages">>),
        stats_schema('live_connections.count', <<"Number of current live connections">>),
        stats_schema('live_connections.max', <<"Historical maximum number of live connections">>),
        stats_schema('retained.count', <<"Number of currently retained messages">>),
        stats_schema('retained.max', <<"Historical maximum number of retained messages">>),
        stats_schema('sessions.count', <<"Number of current sessions">>),
        stats_schema('sessions.max', <<"Historical maximum number of sessions">>),
        stats_schema('suboptions.count', <<"subscriptions.count">>),
        stats_schema('suboptions.max', <<"subscriptions.max">>),
        stats_schema('subscribers.count', <<"Number of current subscribers">>),
        stats_schema('subscribers.max', <<"Historical maximum number of subscribers">>),
        stats_schema(
            'subscriptions.count',
            <<"Number of current subscriptions, including shared subscriptions">>
        ),
        stats_schema('subscriptions.max', <<"Historical maximum number of subscriptions">>),
        stats_schema('subscriptions.shared.count', <<"Number of current shared subscriptions">>),
        stats_schema(
            'subscriptions.shared.max', <<"Historical maximum number of shared subscriptions">>
        ),
        stats_schema('topics.count', <<"Number of current topics">>),
        stats_schema('topics.max', <<"Historical maximum number of topics">>)
    ];
fields(aggergate_data) ->
    [
        {node,
            mk(string(), #{
                desc => <<"Node name">>,
                example => <<"emqx@127.0.0.1">>
            })}
    ] ++ fields(node_stats_data).

stats_schema(Name, Desc) ->
    {Name, mk(non_neg_integer(), #{desc => Desc, example => 0})}.

%%%==============================================================================================
%% api apply
list(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, undefined) of
        true ->
            {200, emqx_mgmt:get_stats()};
        _ ->
            Data = [
                maps:from_list(emqx_mgmt:get_stats(Node) ++ [{node, Node}])
             || Node <- mria_mnesia:running_nodes()
            ],
            {200, Data}
    end.
