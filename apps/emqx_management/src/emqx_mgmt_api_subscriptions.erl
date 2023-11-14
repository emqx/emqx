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
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("hocon/include/hoconsc.hrl").

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
        {rh, hoconsc:mk(integer(), #{desc => <<"Retain Handling">>, example => 0})}
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

format(WhichNode, {{Topic, _Subscriber}, SubOpts}) ->
    maps:merge(
        #{
            topic => emqx_topic:maybe_format_share(Topic),
            clientid => maps:get(subid, SubOpts, null),
            node => WhichNode
        },
        maps:with([qos, nl, rap, rh], SubOpts)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

do_subscriptions_query(QString) ->
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
