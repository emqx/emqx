%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_subscriptions).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([subscriptions/2]).

-export([
    qs2ms/2,
    run_fuzzy_filter/2,
    format/2
]).

-define(SUBS_QSCHEMA, [
    {<<"clientid">>, binary},
    {<<"topic">>, binary},
    {<<"share">>, binary},
    {<<"share_group">>, binary},
    {<<"qos">>, integer},
    {<<"match_topic">>, binary}
]).

namespace() ->
    undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/subscriptions"].

schema("/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get => #{
            description => ?DESC(list_subs),
            tags => [<<"Subscriptions">>],
            parameters => parameters(),
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, subscription)), #{}),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_PARAMETER'], <<"Invalid parameter">>
                ),
                500 => emqx_dashboard_swagger:error_codes(['NODE_DOWN'], <<"Bad RPC">>)
            }
        }
    }.

fields(subscription) ->
    [
        {node, hoconsc:mk(binary(), #{desc => <<"Access type">>, example => <<"emqx@127.0.0.1">>})},
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic name">>, example => <<"testtopic/1">>})},
        {clientid,
            hoconsc:mk(binary(), #{
                desc => <<"Client identifier">>, example => <<"emqx_clientid_xx128cdhfc">>
            })},
        {qos, hoconsc:mk(emqx_schema:qos(), #{desc => <<"QoS">>, example => 0})},
        {nl, hoconsc:mk(integer(), #{desc => <<"No Local">>, example => 0})},
        {rap, hoconsc:mk(integer(), #{desc => <<"Retain as Published">>, example => 0})},
        {rh, hoconsc:mk(integer(), #{desc => <<"Retain Handling">>, example => 0})},
        {durable, hoconsc:mk(boolean(), #{desc => <<"Durable subscription">>, example => false})}
    ].

parameters() ->
    [
        hoconsc:ref(emqx_dashboard_swagger, page),
        hoconsc:ref(emqx_dashboard_swagger, limit),
        {
            node,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Node name">>,
                example => atom_to_list(node())
            })
        },
        {
            clientid,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Client ID">>
            })
        },
        {
            qos,
            hoconsc:mk(emqx_schema:qos(), #{
                in => query,
                required => false,
                desc => <<"QoS">>
            })
        },
        {
            topic,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Topic, url encoding">>
            })
        },
        {
            match_topic,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Match topic string, url encoding">>
            })
        },
        {
            share_group,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Shared subscription group name">>
            })
        },
        {
            durable,
            hoconsc:mk(boolean(), #{
                in => query,
                required => false,
                desc => <<"Filter subscriptions by durability">>
            })
        }
    ].

subscriptions(get, #{query_string := QString}) ->
    Response =
        case check_match_topic(QString) of
            ok -> do_subscriptions_query(QString);
            {error, _} = Err -> Err
        end,
    case Response of
        {error, invalid_match_topic} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"match_topic_invalid">>}};
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end.

format(WhichNode, {{Topic, Subscriber}, SubOpts}) ->
    FallbackClientId =
        case is_binary(Subscriber) of
            true ->
                Subscriber;
            false ->
                %% e.g.: could be a pid...
                null
        end,
    maps:merge(
        #{
            topic => emqx_topic:maybe_format_share(Topic),
            clientid => maps:get(subid, SubOpts, FallbackClientId),
            node => convert_null(WhichNode),
            durable => false
        },
        maps:with([qos, nl, rap, rh, durable], SubOpts)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

convert_null(undefined) -> null;
convert_null(Val) -> Val.

check_match_topic(#{<<"match_topic">> := MatchTopic}) ->
    try emqx_topic:parse(MatchTopic) of
        {#share{}, _} -> {error, invalid_match_topic};
        _ -> ok
    catch
        error:{invalid_topic_filter, _} ->
            {error, invalid_match_topic}
    end;
check_match_topic(_) ->
    ok.

do_subscriptions_query(QString0) ->
    {IsDurable, QString} = maps:take(
        <<"durable">>, maps:merge(#{<<"durable">> => undefined}, QString0)
    ),
    case emqx_persistent_message:is_persistence_enabled() andalso IsDurable of
        false ->
            do_subscriptions_query_mem(QString);
        true ->
            do_subscriptions_query_persistent(QString);
        undefined ->
            merge_queries(
                QString, fun do_subscriptions_query_mem/1, fun do_subscriptions_query_persistent/1
            )
    end.

do_subscriptions_query_mem(QString) ->
    Args = [?SUBOPTION, QString, ?SUBS_QSCHEMA, fun ?MODULE:qs2ms/2, fun ?MODULE:format/2],
    case maps:get(<<"node">>, QString, undefined) of
        undefined ->
            erlang:apply(fun emqx_mgmt_api:cluster_query/5, Args);
        Node0 ->
            case emqx_utils:safe_to_existing_atom(Node0) of
                {ok, Node1} ->
                    erlang:apply(fun emqx_mgmt_api:node_query/6, [Node1 | Args]);
                {error, _} ->
                    {error, Node0, {badrpc, <<"invalid node">>}}
            end
    end.

do_subscriptions_query_persistent(#{<<"page">> := Page, <<"limit">> := Limit} = QString) ->
    Count = emqx_persistent_session_ds_router:stats(n_routes),
    %% TODO: filtering by client ID can be implemented more efficiently:
    FilterTopic = maps:get(<<"topic">>, QString, '_'),
    Stream0 = emqx_persistent_session_ds_router:stream(FilterTopic),
    SubPred = fun(Sub) ->
        compare_optional(<<"topic">>, QString, topic, Sub) andalso
            compare_optional(<<"clientid">>, QString, clientid, Sub) andalso
            compare_optional(<<"qos">>, QString, qos, Sub) andalso
            compare_match_topic_optional(<<"match_topic">>, QString, topic, Sub)
    end,
    NDropped = (Page - 1) * Limit,
    {_, Stream} = consume_n_matching(
        fun persistent_route_to_subscription/1, SubPred, NDropped, Stream0
    ),
    {Subscriptions, Stream1} = consume_n_matching(
        fun persistent_route_to_subscription/1, SubPred, Limit, Stream
    ),
    HasNext = Stream1 =/= [],
    Meta =
        case maps:is_key(<<"match_topic">>, QString) orelse maps:is_key(<<"qos">>, QString) of
            true ->
                %% Fuzzy searches shouldn't return count:
                #{
                    limit => Limit,
                    page => Page,
                    hasnext => HasNext
                };
            false ->
                #{
                    count => Count,
                    limit => Limit,
                    page => Page,
                    hasnext => HasNext
                }
        end,

    #{
        meta => Meta,
        data => Subscriptions
    }.

compare_optional(QField, Query, SField, Subscription) ->
    case Query of
        #{QField := Expected} ->
            maps:get(SField, Subscription) =:= Expected;
        _ ->
            true
    end.

compare_match_topic_optional(QField, Query, SField, Subscription) ->
    case Query of
        #{QField := TopicFilter} ->
            Topic = maps:get(SField, Subscription),
            emqx_topic:match(Topic, TopicFilter);
        _ ->
            true
    end.

%% @doc Drop elements from the stream until encountered N elements
%% matching the predicate function.
-spec consume_n_matching(
    fun((T) -> Q),
    fun((Q) -> boolean()),
    non_neg_integer(),
    emqx_utils_stream:stream(T)
) -> {[Q], emqx_utils_stream:stream(T) | empty}.
consume_n_matching(Map, Pred, N, S) ->
    consume_n_matching(Map, Pred, N, S, []).

consume_n_matching(_Map, _Pred, _N, [], Acc) ->
    {lists:reverse(Acc), []};
consume_n_matching(_Map, _Pred, 0, S, Acc) ->
    case emqx_utils_stream:next(S) of
        [] ->
            {lists:reverse(Acc), []};
        _ ->
            {lists:reverse(Acc), S}
    end;
consume_n_matching(Map, Pred, N, S0, Acc) ->
    case emqx_utils_stream:next(S0) of
        [] ->
            consume_n_matching(Map, Pred, N, [], Acc);
        [Elem | S] ->
            Mapped = Map(Elem),
            case Pred(Mapped) of
                true -> consume_n_matching(Map, Pred, N - 1, S, [Mapped | Acc]);
                false -> consume_n_matching(Map, Pred, N, S, Acc)
            end
    end.

persistent_route_to_subscription(#route{topic = Topic, dest = SessionId}) ->
    case emqx_persistent_session_ds:get_client_subscription(SessionId, Topic) of
        #{subopts := SubOpts} ->
            #{qos := Qos, nl := Nl, rh := Rh, rap := Rap} = SubOpts,
            #{
                topic => Topic,
                clientid => SessionId,
                node => all,

                qos => Qos,
                nl => Nl,
                rh => Rh,
                rap => Rap,
                durable => true
            };
        undefined ->
            #{
                topic => Topic,
                clientid => SessionId,
                node => all,
                durable => true
            }
    end.

%% @private This function merges paginated results from two sources.
%%
%% Note: this implementation is far from ideal: `count' for the
%% queries may be missing, it may be larger than the actual number of
%% elements. This may lead to empty pages that can confuse the user.
%%
%% Not much can be done to mitigate that, though: since the count may
%% be incorrect, we cannot run simple math to determine when one
%% stream begins and another ends: it requires actual iteration.
%%
%% Ideally, the dashboard must be split between durable and mem
%% subscriptions, and this function should be removed for good.
merge_queries(QString0, Q1, Q2) ->
    #{<<"limit">> := Limit, <<"page">> := Page} = QString0,
    C1 = resp_count(QString0, Q1),
    C2 = resp_count(QString0, Q2),
    Meta =
        case is_number(C1) andalso is_number(C2) of
            true ->
                #{
                    count => C1 + C2,
                    limit => Limit,
                    page => Page
                };
            false ->
                #{
                    limit => Limit,
                    page => Page
                }
        end,
    case {C1, C2} of
        {_, 0} ->
            %% The second query is empty. Just return the result of Q1 as usual:
            Q1(QString0);
        {0, _} ->
            %% The first query is empty. Just return the result of Q2 as usual:
            Q2(QString0);
        _ when is_number(C1) ->
            %% Both queries are potentially non-empty, but we at least
            %% have the page number for the first query. We try to
            %% stich the pages together and thus respect the limit
            %% (except for the page where the results switch from Q1
            %% to Q2).

            %% Page where data from the second query is estimated to
            %% begin:
            Q2Page = ceil(C1 / Limit),
            case Page =< Q2Page of
                true ->
                    #{data := Data1, meta := #{hasnext := HN1}} = Q1(QString0),
                    maybe_fetch_from_second_query(#{
                        rows1 => Data1,
                        limit => Limit,
                        hasnext1 => HN1,
                        meta => Meta,
                        count2 => C2,
                        query2 => Q2,
                        query_string => QString0
                    });
                false ->
                    QString = QString0#{<<"page">> => Page - Q2Page},
                    #{data := Data, meta := #{hasnext := HN}} = Q2(QString),
                    #{data => Data, meta => Meta#{hasnext => HN}}
            end;
        _ ->
            %% We don't know how many items is there in the first
            %% query, and the second query is not empty (this includes
            %% the case where `C2' is `undefined'). Best we can do is
            %% to interleave the queries. This may produce less
            %% results per page than `Limit'.
            QString = QString0#{<<"limit">> => ceil(Limit / 2)},
            #{data := D1, meta := #{hasnext := HN1}} = Q1(QString),
            #{data := D2, meta := #{hasnext := HN2}} = Q2(QString),
            #{
                meta => Meta#{hasnext => HN1 or HN2},
                data => D1 ++ D2
            }
    end.

maybe_fetch_from_second_query(Params) ->
    #{
        rows1 := Data1,
        limit := Limit,
        hasnext1 := HN1,
        meta := Meta,
        count2 := C2,
        query2 := Q2,
        query_string := QString0
    } = Params,
    NumRows1 = length(Data1),
    {Data, HN} =
        case (NumRows1 >= Limit) orelse HN1 of
            true ->
                {Data1, HN1 orelse C2 > 0};
            false ->
                #{data := Data2, meta := #{hasnext := HN2}} =
                    Q2(QString0#{<<"limit">> := Limit - NumRows1}),
                {Data1 ++ Data2, HN2}
        end,
    #{
        data => Data,
        meta => Meta#{hasnext => HN}
    }.

resp_count(Query, QFun) ->
    #{meta := Meta} = QFun(Query#{<<"limit">> => 1, <<"page">> => 1}),
    maps:get(count, Meta, undefined).

%%--------------------------------------------------------------------
%% QueryString to MatchSpec (mem sessions)
%%--------------------------------------------------------------------

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {Qs, Fuzzy}) ->
    #{match_spec => gen_match_spec(Qs), fuzzy_fun => fuzzy_filter_fun(Fuzzy)}.

gen_match_spec(Qs) ->
    MtchHead = gen_match_spec(Qs, {{'_', '_'}, #{}}),
    [{MtchHead, [], ['$_']}].

gen_match_spec([], MtchHead) ->
    MtchHead;
gen_match_spec([{Key, '=:=', Value} | More], MtchHead) ->
    gen_match_spec(More, update_ms(Key, Value, MtchHead)).

update_ms(clientid, X, {{Topic, Pid}, Opts}) ->
    {{Topic, Pid}, Opts#{subid => X}};
update_ms(topic, X, {{Topic, Pid}, Opts}) when
    is_record(Topic, share)
->
    {{#share{group = '_', topic = X}, Pid}, Opts};
update_ms(topic, X, {{Topic, Pid}, Opts}) when
    is_binary(Topic) orelse Topic =:= '_'
->
    {{X, Pid}, Opts};
update_ms(share_group, X, {{Topic, Pid}, Opts}) when
    not is_record(Topic, share)
->
    {{#share{group = X, topic = Topic}, Pid}, Opts};
update_ms(qos, X, {{Topic, Pid}, Opts}) ->
    {{Topic, Pid}, Opts#{qos => X}}.

fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {{SubedTopic, _}, _}, [{topic, match, TopicFilter} | Fuzzy]) ->
    emqx_topic:match(SubedTopic, TopicFilter) andalso run_fuzzy_filter(E, Fuzzy).
