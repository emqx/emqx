%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([slow_subs/2, get_history/0, settings/2]).
-define(TAGS, [<<"Slow Subscriptions">>]).

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_mgmt_util, [bad_request/0]).

-define(APP, emqx_slow_subs).
-define(APP_NAME, <<"emqx_slow_subs">>).
-define(DEFAULT_RPC_TIMEOUT, timer:seconds(5)).

namespace() -> "slow_subscribers_statistics".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/slow_subscriptions", "/slow_subscriptions/settings"].

schema(("/slow_subscriptions")) ->
    #{
        'operationId' => slow_subs,
        delete => #{
            tags => ?TAGS,
            description => ?DESC(clear_records_api),
            parameters => [],
            'requestBody' => [],
            responses => #{204 => <<"No Content">>}
        },
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_records_api),
            parameters => [
                ref(emqx_dashboard_swagger, page),
                ref(emqx_dashboard_swagger, limit)
            ],
            'requestBody' => [],
            responses => #{200 => [{data, mk(hoconsc:array(ref(record)), #{})}]}
        }
    };
schema("/slow_subscriptions/settings") ->
    #{
        'operationId' => settings,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_setting_api),
            responses => #{200 => conf_schema()}
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(update_setting_api),
            'requestBody' => conf_schema(),
            responses => #{200 => conf_schema()}
        }
    }.

fields(record) ->
    [
        {clientid, mk(string(), #{desc => ?DESC(clientid)})},
        {node, mk(string(), #{desc => ?DESC(node)})},
        {topic, mk(string(), #{desc => ?DESC(topic)})},
        {timespan,
            mk(
                integer(),
                #{desc => ?DESC(timespan)}
            )},
        {last_update_time, mk(integer(), #{desc => ?DESC(last_update_time)})}
    ].

conf_schema() ->
    Ref = hoconsc:ref(emqx_slow_subs_schema, "slow_subs"),
    hoconsc:mk(Ref, #{}).

slow_subs(delete, _) ->
    _ = rpc_call(fun(Nodes) -> emqx_slow_subs_proto_v1:clear_history(Nodes) end),
    {204};
slow_subs(get, _) ->
    NodeRankL = rpc_call(fun(Nodes) -> emqx_slow_subs_proto_v1:get_history(Nodes) end),
    Fun = fun
        ({ok, L}, Acc) -> L ++ Acc;
        (_, Acc) -> Acc
    end,
    RankL = lists:foldl(Fun, [], NodeRankL),

    SortFun = fun(#{timespan := A}, #{timespan := B}) ->
        A > B
    end,

    SortedL = lists:sort(SortFun, RankL),
    SortedL2 = lists:sublist(SortedL, ?MAX_SIZE),

    {200, #{data => SortedL2}}.

get_history() ->
    Node = node(),
    RankL = ets:tab2list(?TOPK_TAB),
    ConvFun = fun(
        #top_k{
            index = ?TOPK_INDEX(TimeSpan, ?ID(ClientId, Topic)),
            last_update_time = LastUpdateTime
        }
    ) ->
        #{
            clientid => ClientId,
            node => Node,
            topic => Topic,
            timespan => TimeSpan,
            last_update_time => LastUpdateTime
        }
    end,

    lists:map(ConvFun, RankL).

settings(get, _) ->
    {200, emqx:get_raw_config([slow_subs], #{})};
settings(put, #{body := Body}) ->
    case emqx_slow_subs:update_settings(Body) of
        {ok, #{raw_config := NewConf}} ->
            {200, NewConf};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update slow subs config failed ~p", [Reason])),
            {400, 'BAD_REQUEST', Message}
    end.

rpc_call(Fun) ->
    Nodes = mria:running_nodes(),
    Fun(Nodes).
