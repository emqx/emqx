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

-behavior(minirest_api).

-export([api_spec/0]).

-export([alarms/2]).

-export([ query_activated/3
        , query_deactivated/3]).
%% notice: from emqx_alarms
-define(ACTIVATED_ALARM, emqx_activated_alarm).
-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).

api_spec() ->
    {[alarms_api()], [alarm_schema()]}.

alarm_schema() ->
    #{
        alarm => #{
            type => object,
            properties => #{
                node => #{
                    type => string,
                    description => <<"Alarm in node">>},
                name => #{
                    type => string,
                    description => <<"Alarm name">>},
                message => #{
                    type => string,
                    description => <<"Alarm readable information">>},
                details => #{
                    type => object,
                    description => <<"Alarm detail">>},
                duration => #{
                    type => integer,
                    description => <<"Alarms duration time; UNIX time stamp">>}
            }
        }
    }.

alarms_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X alarms",
            parameters => [#{
                name => activated,
                in => query,
                description => <<"All alarms, if not specified">>,
                required => false,
                schema => #{type => boolean, default => true}
            }],
            responses => #{
                <<"200">> =>
                emqx_mgmt_util:response_array_schema(<<"List all alarms">>, <<"alarm">>)}},
        delete => #{
            description => "Remove all deactivated alarms",
            responses => #{
                <<"200">> =>
                emqx_mgmt_util:response_schema(<<"Remove all deactivated alarms ok">>)}}},
    {"/alarms", Metadata, alarms}.

%%%==============================================================================================
%% parameters trans
alarms(get, Request) ->
    case proplists:get_value(<<"activated">>, cowboy_req:parse_qs(Request), undefined) of
        undefined ->
            list(#{activated => undefined});
        <<"true">> ->
            list(#{activated => true});
        <<"false">> ->
            list(#{activated => false})
    end;

alarms(delete, _Request) ->
    delete().

%%%==============================================================================================
%% api apply
list(#{activated := true}) ->
    do_list(activated);
list(#{activated := false}) ->
    do_list(deactivated);
list(#{activated := undefined}) ->
    do_list(activated).

delete() ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {200}.

%%%==============================================================================================
%% internal
do_list(Type) ->
    {Table, Function} =
        case Type of
            activated ->
                {?ACTIVATED_ALARM, query_activated};
            deactivated ->
                {?DEACTIVATED_ALARM, query_deactivated}
        end,
    Response = emqx_mgmt_api:cluster_query([], {Table, []}, {?MODULE, Function}),
    {200, Response}.

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
