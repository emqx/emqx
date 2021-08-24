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

-export([ query_activated/3
        , query_deactivated/3]).
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
        {duration, integer, <<"Alarms duration time; UNIX time stamp">>}
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
alarms(get, Request) ->
    Params = cowboy_req:parse_qs(Request),
    list(Params);

alarms(delete, _Request) ->
    delete().

%%%==============================================================================================
%% api apply
list(Params) ->
    {Table, Function} =
        case proplists:get_value(<<"activated">>, Params, <<"true">>) of
            <<"true">> ->
                {?ACTIVATED_ALARM, query_activated};
            <<"false">> ->
                {?DEACTIVATED_ALARM, query_deactivated}
        end,
    Params1 = proplists:delete(<<"activated">>, Params),
    Response = emqx_mgmt_api:cluster_query(Params1, {Table, []}, {?MODULE, Function}),
    {200, Response}.

delete() ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {200}.

%%%==============================================================================================
%% internal
query_activated(_, Start, Limit) ->
    query(?ACTIVATED_ALARM, Start, Limit).

query_deactivated(_, Start, Limit) ->
    query(?DEACTIVATED_ALARM, Start, Limit).

query(Table, Start, Limit) ->
    Ms = [{'$1',[],['$1']}],
    emqx_mgmt_api:select_table(Table, Ms, Start, Limit, fun format_alarm/1).

format_alarm(Alarms) when is_list(Alarms) ->
    [emqx_alarm:format(Alarm) || Alarm <- Alarms];

format_alarm(Alarm) ->
    emqx_alarm:format(Alarm).
