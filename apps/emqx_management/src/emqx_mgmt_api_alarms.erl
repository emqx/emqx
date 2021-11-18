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

-module(emqx_mgmt_api_alarms).

-behaviour(minirest_api).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1]).

-export([alarms/2]).

%% internal export (for query)
-export([query/4]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/alarms"].

schema("/alarms") ->
    #{
        'operationId' => alarms,
        get => #{
            description => <<"EMQ X alarms">>,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit),
                {activated, hoconsc:mk(boolean(), #{in => query,
                    desc => <<"All alarms, if not specified">>,
                    nullable => true})}
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, alarm)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(?MODULE, meta), #{})}
                ]
            }
        },
        delete  => #{
            description => <<"Remove all deactivated alarms">>,
            responses => #{
                204 => <<"Remove all deactivated alarms ok">>
            }
        }
    }.

fields(alarm) ->
    [
        {node, hoconsc:mk(binary(),
                          #{desc => <<"Alarm in node">>, example => atom_to_list(node())})},
        {name, hoconsc:mk(binary(),
                          #{desc => <<"Alarm name">>, example => <<"high_system_memory_usage">>})},
        {message, hoconsc:mk(binary(), #{desc => <<"Alarm readable information">>,
            example => <<"System memory usage is higher than 70%">>})},
        {details, hoconsc:mk(map(), #{desc => <<"Alarm details information">>,
            example => #{<<"high_watermark">> => 70}})},
        {duration, hoconsc:mk(integer(),
                              #{desc => <<"Alarms duration time; UNIX time stamp, millisecond">>,
            example => 297056})},
        {activate_at, hoconsc:mk(binary(), #{desc => <<"Alarms activate time, RFC 3339">>,
            example => <<"2021-10-25T11:52:52.548+08:00">>})},
        {deactivate_at, hoconsc:mk(binary(),
                                   #{desc => <<"Nullable, alarms deactivate time, RFC 3339">>,
            example => <<"2021-10-31T10:52:52.548+08:00">>})}
    ];

fields(meta) ->
    emqx_dashboard_swagger:fields(page) ++
        emqx_dashboard_swagger:fields(limit) ++
        [{count, hoconsc:mk(integer(), #{example => 1})}].
%%%==============================================================================================
%% parameters trans
alarms(get, #{query_string := Qs}) ->
    Table =
        case maps:get(<<"activated">>, Qs, true) of
            true -> ?ACTIVATED_ALARM;
            false -> ?DEACTIVATED_ALARM
        end,
    Response = emqx_mgmt_api:cluster_query(Qs, Table, [], {?MODULE, query}),
    emqx_mgmt_util:generate_response(Response);

alarms(delete, _Params) ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {204}.

%%%==============================================================================================
%% internal

query(Table, _QsSpec, Continuation, Limit) ->
    Ms = [{'$1',[],['$1']}],
    emqx_mgmt_api:select_table_with_count(Table, Ms, Continuation, Limit, fun format_alarm/1).

format_alarm(Alarms) when is_list(Alarms) ->
    [emqx_alarm:format(Alarm) || Alarm <- Alarms];

format_alarm(Alarm) ->
    emqx_alarm:format(Alarm).
