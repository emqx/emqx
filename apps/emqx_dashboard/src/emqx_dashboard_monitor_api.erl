%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").

-behaviour(minirest_api).

-export([api_spec/0]).

-export([counters/2, current_counters/2]).

-define(COUNTERS, [ connection
                  , route
                  , subscriptions
                  , received
                  , sent
                  , dropped]).

api_spec() ->
    {[monitor_api(), monitor_current_api()], [counters_schema()]}.

monitor_api() ->
    Metadata = #{
        get => #{
            description => <<"List monitor data">>,
            parameters => [
                #{
                    name => node,
                    in => query,
                    required => false,
                    schema => #{type => string}
                },
                #{
                    name => counter,
                    in => query,
                    required => false,
                    schema => #{type => string, enum => ?COUNTERS}
                }
            ],
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"Monitor count data">>, counters)}}},
    {"/monitor", Metadata, counters}.
monitor_current_api() ->
    Metadata = #{
        get => #{
            description => <<"Current monitor data">>,
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"Current monitor data">>,
                    current_counters_schema())}}},
    {"/monitor/current", Metadata, current_counters}.

current_counters_schema() ->
    #{
        type => object,
        properties => #{
            connection => #{type => integer},
            sent => #{type => integer},
            received => #{type => integer},
            subscription => #{type => integer}}
    }.

counters_schema() ->
    Fun =
        fun(K, M) ->
            maps:merge(M, counters_schema(K))
        end,
    Properties = lists:foldl(Fun, #{}, ?COUNTERS),
    #{
        counters => #{
            type => object,
            properties => Properties}
    }.

counters_schema(Name) ->
    #{Name => #{
        type => array,
        items => #{
            type => object,
            properties => #{
                timestamp => #{
                    type => integer},
                count => #{
                    type => integer}}}}}.
%%%==============================================================================================
%% parameters trans
counters(get, Request) ->
    case cowboy_req:parse_qs(Request) of
        [] ->
            {200, get_collect()};
        Params ->
            lookup(Params)
    end.

current_counters(get, _) ->
    Data = [get_collect(Node) || Node <- ekka_mnesia:running_nodes()],
    Nodes = length(ekka_mnesia:running_nodes()),
    {Received, Sent, Sub, Conn} = format_current_metrics(Data),
    Response = #{
        nodes           => Nodes,
        received        => Received,
        sent            => Sent,
        subscription    => Sub,
        connection      => Conn
    },
    {200, Response}.

    %%%==============================================================================================
%% api apply

lookup(Params) ->
    Fun =
        fun({K,V}, M) ->
            maps:put(binary_to_atom(K, utf8), binary_to_atom(V, utf8), M)
        end,
    lookup_(lists:foldl(Fun, #{}, Params)).

lookup_(#{node := Node, counter := Counter}) ->
    {200, sampling(Node, Counter)};
lookup_(#{node := Node}) ->
    {200, sampling(Node)};
lookup_(#{counter := Counter}) ->
    Data = [sampling(Node, Counter) || Node <- ekka_mnesia:running_nodes()],
    {200, Data}.

format_current_metrics(Collects) ->
    format_current_metrics(Collects, {0,0,0,0}).
format_current_metrics([], Acc) ->
    Acc;
format_current_metrics([{Received, Sent, Sub, Conn} | Collects], {Received1, Sent1, Sub1, Conn1}) ->
    format_current_metrics(Collects, {Received1 + Received, Sent1 + Sent, Sub1 + Sub, Conn1 + Conn}).

get_collect() ->
    Counters = [sampling(Node) || Node <- ekka_mnesia:running_nodes()],
    merger_counters(Counters).

get_collect(Node) when Node =:= node() ->
    emqx_dashboard_collection:get_collect();
get_collect(Node) ->
    case rpc:call(Node, emqx_dashboard_collection, get_collect, []) of
        {badrpc, _Reason} -> #{};
        Res -> Res
    end.

merger_counters(ClusterCounters) ->
    lists:foldl(fun merger_node_counters/2, #{}, ClusterCounters).

merger_node_counters(NodeCounters, Counters) ->
    maps:fold(fun merger_counter/3, Counters, NodeCounters).

merger_counter(Key, Counters, Res) ->
    case maps:get(Key, Res, undefined) of
        undefined ->
            Res#{Key => Counters};
        OldCounters ->
            NCounters = lists:foldl(fun merger_counter/2, OldCounters, Counters),
            Res#{Key => NCounters}
    end.

merger_counter(#{timestamp := Timestamp, count := Value}, Counters) ->
    Comparison =
        fun(Counter) ->
            case maps:get(timestamp, Counter) =:= Timestamp of
                true ->
                    Count = maps:get(count, Counter),
                    {ok, Counter#{count => Count + Value}};
                false ->
                    ignore
            end
        end,
    key_replace(Counters, Comparison, #{timestamp => Timestamp, count => Value}).

key_replace(List, Comparison, Default) ->
    key_replace(List, List, Comparison, Default).

key_replace([], All, _Comparison, Default) ->
    [Default | All];

key_replace([Term | List], All, Comparison, Default) ->
    case Comparison(Term) of
        {ok, NTerm} ->
            Tail = [NTerm | List],
            Header = lists:sublist(All, length(All) - length(Tail)),
            lists:append(Header, Tail);
        _ ->
            key_replace(List, All, Comparison, Default)
    end.

sampling(Node) when Node =:= node() ->
    Time = emqx_dashboard_collection:get_local_time() - 7200000,
    All = dets:select(emqx_collect, [{{mqtt_collect,'$1','$2'}, [{'>', '$1', Time}], ['$_']}]),
    format(lists:sort(All));
sampling(Node) ->
    rpc:call(Node, ?MODULE, sampling, [Node]).

sampling(Node, Counter) when Node =:= node() ->
    Time = emqx_dashboard_collection:get_local_time() - 7200000,
    All = dets:select(emqx_collect, [{{mqtt_collect,'$1','$2'}, [{'>', '$1', Time}], ['$_']}]),
    format_single(lists:sort(All), Counter);
sampling(Node, Counter) ->
    rpc:call(Node, ?MODULE, sampling, [Node, Counter]).

format(Collects) ->
    format(Collects, {[],[],[],[],[],[]}).
format([], {Connection, Route, Subscription, Received, Sent, Dropped}) ->
    #{
        connection      => add_key(Connection),
        route           => add_key(Route),
        subscriptions   => add_key(Subscription),
        received        => add_key(Received),
        sent            => add_key(Sent),
        dropped         => add_key(Dropped)
    };

format([#mqtt_collect{timestamp = Ts, collect = {C, R, S, Re, S1, D}} | Collects],
       {Connection, Route, Subscription, Received, Sent, Dropped}) ->
    format(Collects, {[[Ts, C]  | Connection],
                      [[Ts, R]  | Route],
                      [[Ts, S]  | Subscription],
                      [[Ts, Re] | Received],
                      [[Ts, S1] | Sent],
                      [[Ts, D]  | Dropped]}).
add_key(Collects) ->
    lists:reverse([#{timestamp => Ts, count => C} || [Ts, C] <- Collects]).

format_single(Collects, Counter) ->
    #{Counter => format_single(Collects, counter_index(Counter), [])}.
format_single([], _Index, Acc) ->
    lists:reverse(Acc);
format_single([#mqtt_collect{timestamp = Ts, collect = Collect} | Collects], Index, Acc) ->
    format_single(Collects, Index,
        [#{timestamp => Ts, count => erlang:element(Index, Collect)} | Acc]).

counter_index(connection)    -> 1;
counter_index(route)         -> 2;
counter_index(subscriptions) -> 3;
counter_index(received)      -> 4;
counter_index(sent)          -> 5;
counter_index(dropped)       -> 6.
