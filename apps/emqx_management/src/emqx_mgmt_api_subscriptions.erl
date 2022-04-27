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

-module(emqx_mgmt_api_subscriptions).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([subscriptions/2]).

-export([
    query/4,
    format/1
]).

-define(SUBS_QTABLE, emqx_suboption).

-define(SUBS_QSCHEMA, [
    {<<"clientid">>, binary},
    {<<"topic">>, binary},
    {<<"share">>, binary},
    {<<"share_group">>, binary},
    {<<"qos">>, integer},
    {<<"match_topic">>, binary}
]).

-define(QUERY_FUN, {?MODULE, query}).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/subscriptions"].

schema("/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get => #{
            description => <<"List subscriptions">>,
            parameters => parameters(),
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, subscription)), #{})
            }
        }
    }.

fields(subscription) ->
    [
        {node, hoconsc:mk(binary(), #{desc => <<"Access type">>})},
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic name">>})},
        {clientid, hoconsc:mk(binary(), #{desc => <<"Client identifier">>})},
        {qos, hoconsc:mk(emqx_schema:qos(), #{desc => <<"QoS">>})},
        {nl, hoconsc:mk(integer(), #{desc => <<"No Local">>})},
        {rap, hoconsc:mk(integer(), #{desc => <<"Retain as Published">>})},
        {rh, hoconsc:mk(integer(), #{desc => <<"Retain Handling">>})}
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
        }
    ].

subscriptions(get, #{query_string := QString}) ->
    Response =
        case maps:get(<<"node">>, QString, undefined) of
            undefined ->
                emqx_mgmt_api:cluster_query(
                    QString,
                    ?SUBS_QTABLE,
                    ?SUBS_QSCHEMA,
                    ?QUERY_FUN
                );
            Node0 ->
                emqx_mgmt_api:node_query(
                    binary_to_atom(Node0, utf8),
                    QString,
                    ?SUBS_QTABLE,
                    ?SUBS_QSCHEMA,
                    ?QUERY_FUN
                )
        end,
    case Response of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end.

format(Items) when is_list(Items) ->
    [format(Item) || Item <- Items];
format({{Subscriber, Topic}, Options}) ->
    format({Subscriber, Topic, Options});
format({_Subscriber, Topic, Options}) ->
    maps:merge(
        #{
            topic => get_topic(Topic, Options),
            clientid => maps:get(subid, Options),
            node => node()
        },
        maps:with([qos, nl, rap, rh], Options)
    ).

get_topic(Topic, #{share := Group}) ->
    filename:join([<<"$share">>, Group, Topic]);
get_topic(Topic, _) ->
    Topic.

%%--------------------------------------------------------------------
%% Query Function
%%--------------------------------------------------------------------

query(Tab, {Qs, []}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format/1
    );
query(Tab, {Qs, Fuzzy}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    FuzzyFilterFun = fuzzy_filter_fun(Fuzzy),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format/1
    ).

fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {{_, Topic}, _}, [{topic, match, TopicFilter} | Fuzzy]) ->
    emqx_topic:match(Topic, TopicFilter) andalso run_fuzzy_filter(E, Fuzzy).

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
update_ms(share_group, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{share => X}};
update_ms(qos, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{qos => X}}.
