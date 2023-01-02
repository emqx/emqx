%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").

-define(SUBS_QS_SCHEMA, {emqx_suboption,
            [{<<"clientid">>, binary},
             {<<"topic">>, binary},
             {<<"share">>, binary},
             {<<"qos">>, integer},
             {<<"_match_topic">>, binary}]}).

-rest_api(#{name   => list_subscriptions,
            method => 'GET',
            path   => "/subscriptions/",
            func   => list,
            descr  => "A list of subscriptions in the cluster"}).

-rest_api(#{name   => list_node_subscriptions,
            method => 'GET',
            path   => "/nodes/:atom:node/subscriptions/",
            func   => list,
            descr  => "A list of subscriptions on a node"}).

-rest_api(#{name   => lookup_client_subscriptions,
            method => 'GET',
            path   => "/subscriptions/:bin:clientid",
            func   => lookup,
            descr  => "A list of subscriptions of a client"}).

-rest_api(#{name   => lookup_client_subscriptions_with_node,
            method => 'GET',
            path   => "/nodes/:atom:node/subscriptions/:bin:clientid",
            func   => lookup,
            descr  => "A list of subscriptions of a client on the node"}).

-export([ list/2
        , lookup/2
        ]).

-export([ query/3
        , format/1
        ]).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format}).

list(Bindings, Params) when map_size(Bindings) == 0 ->
    case proplists:get_value(<<"topic">>, Params) of
        undefined ->
            minirest:return({ok, emqx_mgmt_api:cluster_query(Params, ?SUBS_QS_SCHEMA, ?query_fun)});
        Topic0 ->
            Topic = emqx_mgmt_util:urldecode(Topic0),
            Data = emqx_mgmt:list_subscriptions_via_topic(Topic, ?format_fun),
            FilterData = filter_subscriptions(Data, Params),
            minirest:return({ok, add_meta(Params, FilterData)})
    end;

list(#{node := Node} = Bindings, Params) ->
    case proplists:get_value(<<"topic">>, Params) of
        undefined ->
            case Node =:= node() of
                true ->
                    minirest:return({ok, emqx_mgmt_api:node_query(Node, Params, ?SUBS_QS_SCHEMA, ?query_fun)});
                false ->
                    case rpc:call(Node, ?MODULE, list, [Bindings, Params]) of
                        {badrpc, Reason} -> minirest:return({error, Reason});
                        Res -> Res
                    end
            end;
        Topic0 ->
            Topic = emqx_mgmt_util:urldecode(Topic0),
            Data = emqx_mgmt:list_subscriptions_via_topic(Node, Topic, ?format_fun),
            FilterData = filter_subscriptions(Data, Params),
            minirest:return({ok, add_meta(Params, FilterData)})
    end.

add_meta(Params, List) ->
    Page = emqx_mgmt_api:page(Params),
    Limit = emqx_mgmt_api:limit(Params),
    Count = erlang:length(List),
    Start = (Page - 1) * Limit + 1,
    Data = lists:sublist(List, Start, Limit),
    #{meta => #{
        page => Page,
        limit => Limit,
        hasnext => Start + Limit - 1 < Count,
        count => Count},
        data => Data,
        code => 0
    }.

lookup(#{node := Node, clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_subscriptions(Node, emqx_mgmt_util:urldecode(ClientId), ?format_fun)});

lookup(#{clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_subscriptions(emqx_mgmt_util:urldecode(ClientId), ?format_fun)}).

format(Items) when is_list(Items) ->
    [format(Item) || Item <- Items];

format({{Subscriber, Topic}, Options}) ->
    format({Subscriber, Topic, Options});

format({_Subscriber, Topic, Options = #{share := Group}}) ->
    QoS = maps:get(qos, Options),
    #{node => node(), topic => filename:join([<<"$share">>, Group, Topic]), clientid => maps:get(subid, Options), qos => QoS};
format({_Subscriber, Topic, Options}) ->
    QoS = maps:get(qos, Options),
    #{node => node(), topic => Topic, clientid => maps:get(subid, Options, ""), qos => QoS}.

%%--------------------------------------------------------------------
%% Query Function
%%--------------------------------------------------------------------

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(emqx_suboption, Ms, Start, Limit, fun format/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(emqx_suboption, MatchFun, Start, Limit, fun format/1).

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    fun(Rows) ->
         case ets:match_spec_run(Rows, MsC) of
             [] -> [];
             Ls -> lists:filter(fun(E) -> run_fuzzy_match(E, Fuzzy) end, Ls)
         end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {{_, Topic}, _}, [{topic, match, TopicFilter}|Fuzzy]) ->
    emqx_topic:match(Topic, TopicFilter) andalso run_fuzzy_match(E, Fuzzy).

%%--------------------------------------------------------------------
%% Query String to Match Spec

qs2ms(Qs) ->
    MtchHead = qs2ms(Qs, {{'_', '_'}, #{}}),
    [{MtchHead, [], ['$_']}].

qs2ms([], MtchHead) ->
    MtchHead;
qs2ms([{Key, '=:=', Value} | More], MtchHead) ->
    qs2ms(More, update_ms(Key, Value, MtchHead)).

update_ms(clientid, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{subid => X}};
update_ms(topic, X, {{Pid, _Topic}, Opts}) ->
    {{Pid, X}, Opts};
update_ms(share, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{share => X}};
update_ms(qos, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{qos => X}}.

filter_subscriptions(Data0, Params) ->
    Data1 = filter_by_key(qos, qos(Params), Data0),
    Data2 = filter_by_key(clientid, proplists:get_value(<<"clientid">>, Params), Data1),
    case proplists:get_value(<<"share">>, Params) of
        undefined -> Data2;
        Share ->
            Prefix = filename:join([<<"$share">>, Share]),
            Size = byte_size(Prefix),
            lists:filter(fun(#{topic := Topic}) ->
                case Topic of
                    <<Prefix:Size/binary, _/binary>> -> true;
                    _ -> false
                end
                         end,
                Data2)
    end.

qos(Params) ->
    case proplists:get_value(<<"qos">>, Params) of
        undefined -> undefined;
        Qos when is_integer(Qos) -> Qos;
        Qos when is_binary(Qos) -> binary_to_integer(Qos)
    end.

filter_by_key(_Key, undefined, List) -> List;
filter_by_key(Key, Value, List) -> lists:filter(fun(E) -> Value =:= maps:get(Key, E) end, List).
