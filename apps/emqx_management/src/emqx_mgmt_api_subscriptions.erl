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
    qs2ms/2,
    run_fuzzy_filter/2,
    format/2
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

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/subscriptions"].

schema("/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get => #{
            description => <<"List subscriptions">>,
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
                    ?SUBS_QTABLE,
                    QString,
                    ?SUBS_QSCHEMA,
                    fun ?MODULE:qs2ms/2,
                    fun ?MODULE:format/2
                );
            Node0 ->
                case emqx_misc:safe_to_existing_atom(Node0) of
                    {ok, Node1} ->
                        emqx_mgmt_api:node_query(
                            Node1,
                            ?SUBS_QTABLE,
                            QString,
                            ?SUBS_QSCHEMA,
                            fun ?MODULE:qs2ms/2,
                            fun ?MODULE:format/2
                        );
                    {error, _} ->
                        {error, Node0, {badrpc, <<"invalid node">>}}
                end
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

format(WhichNode, {{Topic, _Subscriber}, Options}) ->
    maps:merge(
        #{
            topic => get_topic(Topic, Options),
            clientid => maps:get(subid, Options, null),
            node => WhichNode
        },
        maps:with([qos, nl, rap, rh], Options)
    ).

get_topic(Topic, #{share := Group}) ->
    emqx_topic:join([<<"$share">>, Group, Topic]);
get_topic(Topic, _) ->
    Topic.

%%--------------------------------------------------------------------
%% QueryString to MatchSpec
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
update_ms(topic, X, {{_Topic, Pid}, Opts}) ->
    {{X, Pid}, Opts};
update_ms(share_group, X, {{Topic, Pid}, Opts}) ->
    {{Topic, Pid}, Opts#{share => X}};
update_ms(qos, X, {{Topic, Pid}, Opts}) ->
    {{Topic, Pid}, Opts#{qos => X}}.

fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {{Topic, _}, _}, [{topic, match, TopicFilter} | Fuzzy]) ->
    emqx_topic:match(Topic, TopicFilter) andalso run_fuzzy_filter(E, Fuzzy).
