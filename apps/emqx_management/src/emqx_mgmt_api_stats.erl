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
-module(emqx_mgmt_api_stats).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([list/2]).

api_spec() ->
    {[stats_api()], stats_schema()}.

stats_schema() ->
    Stats = #{
        type => array,
        items => #{
            type => object,
            properties => emqx_mgmt_util:properties([{'node', string} | properties()])
        }
    },
    Stat = #{
        type => object,
        properties => emqx_mgmt_util:properties(properties())
    },
    StatsInfo =#{
        oneOf => [ minirest:ref(stats)
                 , minirest:ref(stat)
                 ]
    },
    [#{stats => Stats, stat => Stat, stats_info => StatsInfo}].

properties() ->
    [
        {'channels.count',             integer, <<"sessions.count">>},
        {'channels.max',               integer, <<"session.max">>},
        {'connections.count',          integer, <<"Number of current connections">>},
        {'connections.max',            integer, <<"Historical maximum number of connections">>},
        {'retained.count',             integer, <<"Number of currently retained messages">>},
        {'retained.max',               integer, <<"Historical maximum number of retained messages">>},
        {'routes.count',               integer, <<"Number of current routes">>},
        {'routes.max',                 integer, <<"Historical maximum number of routes">>},
        {'sessions.count',             integer, <<"Number of current sessions">>},
        {'sessions.max',               integer, <<"Historical maximum number of sessions">>},
        {'suboptions.count',           integer, <<"subscriptions.count">>},
        {'suboptions.max',             integer, <<"subscriptions.max">>},
        {'subscribers.count',          integer, <<"Number of current subscribers">>},
        {'subscribers.max',            integer, <<"Historical maximum number of subscribers">>},
        {'subscriptions.count',        integer, <<"Number of current subscriptions, including shared subscriptions">>},
        {'subscriptions.max',          integer, <<"Historical maximum number of subscriptions">>},
        {'subscriptions.shared.count', integer, <<"Number of current shared subscriptions">>},
        {'subscriptions.shared.max',   integer, <<"Historical maximum number of shared subscriptions">>},
        {'topics.count',               integer, <<"Number of current topics">>},
        {'topics.max',                 integer, <<"Historical maximum number of topics">>}
    ].

stats_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X stats">>,
            parameters => [#{
                name => aggregate,
                in => query,
                schema => #{type => boolean}
            }],
            responses => #{
                <<"200">> => #{
                    description => <<"List stats ok">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"stats_info">>)
                        }
                    }
                }
            }}},
    {"/stats", Metadata, list}.

%%%==============================================================================================
%% api apply
list(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, undefined) of
        <<"true">> ->
            {200, emqx_mgmt:get_stats()};
        _ ->
            Data = [maps:from_list(emqx_mgmt:get_stats(Node) ++ [{node, Node}]) ||
                        Node <- ekka_mnesia:running_nodes()],
            {200, Data}
    end.
