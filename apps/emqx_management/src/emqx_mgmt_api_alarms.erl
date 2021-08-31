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

-export([api_spec/0]).

-export([alarms/2]).

%% internal export (for query)
-export([ query/4
        ]).

%% notice: from emqx_alarms
-define(ACTIVATED_ALARM, emqx_activated_alarm).
-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).

-import(emqx_mgmt_util, [ object_array_schema/2
                        , schema/1
                        , properties/1
                        ]).

api_spec() ->
    {[alarms_api()], []}.

properties() ->
    properties([
        {node, string, <<"Alarm in node">>},
        {name, string, <<"Alarm name">>},
        {message, string, <<"Alarm readable information">>},
        {details, object},
        {duration, integer, <<"Alarms duration time; UNIX time stamp, millisecond">>},
        {activate_at, string, <<"Alarms activate time, RFC 3339">>},
        {deactivate_at, string, <<"Nullable, alarms deactivate time, RFC 3339">>}
    ]).

alarms_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X alarms">>,
            parameters => emqx_mgmt_util:page_params() ++ [#{
                name => activated,
                in => query,
                description => <<"All alarms, if not specified">>,
                required => false,
                schema => #{type => boolean, default => true}
            }],
            responses => #{
                <<"200">> =>
                object_array_schema(properties(), <<"List all alarms">>)}},
        delete => #{
            description => <<"Remove all deactivated alarms">>,
            responses => #{
                <<"200">> =>
                schema(<<"Remove all deactivated alarms ok">>)}}},
    {"/alarms", Metadata, alarms}.

%%%==============================================================================================
%% parameters trans
alarms(get, #{query_string := Qs}) ->
    Table =
        case maps:get(<<"activated">>, Qs, <<"true">>) of
            <<"true">> -> ?ACTIVATED_ALARM;
            <<"false">> -> ?DEACTIVATED_ALARM
        end,
    Response = emqx_mgmt_api:cluster_query(Qs, Table, [], {?MODULE, query}),
    {200, Response};

alarms(delete, _Params) ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {200}.

%%%==============================================================================================
%% internal

query(Table, _QsSpec, Start, Limit) ->
    Ms = [{'$1',[],['$1']}],
    emqx_mgmt_api:select_table(Table, Ms, Start, Limit, fun format_alarm/1).

format_alarm(Alarms) when is_list(Alarms) ->
    [emqx_alarm:format(Alarm) || Alarm <- Alarms];

format_alarm(Alarm) ->
    emqx_alarm:format(Alarm).
