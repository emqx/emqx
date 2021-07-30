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

-module(emqx_mgmt_api_configs).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([brokers/2]).

api_spec() ->
    {[config_apis()], config_schemas()}.

config_schema() -> [
    #{
        broker => #{
            type => object,
        }
    }
|| RootKey <- ].

config_apis() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X configs">>,
            parameters => [#{
                name => activated,
                in => query,
                description => <<"All configs, if not specified">>,
                required => false,
                schema => #{type => boolean, default => true}
            }],
            responses => #{
                <<"200">> =>
                emqx_mgmt_util:response_array_schema(<<"List all configs">>, config)}},
        delete => #{
            description => <<"Remove all deactivated configs">>,
            responses => #{
                <<"200">> =>
                emqx_mgmt_util:response_schema(<<"Remove all deactivated configs ok">>)}}},
    {"/configs", Metadata, configs}.

%%%==============================================================================================
%% parameters trans
configs(get, Request) ->
    case proplists:get_value(<<"activated">>, cowboy_req:parse_qs(Request), undefined) of
        undefined ->
            list(#{activated => undefined});
        <<"true">> ->
            list(#{activated => true});
        <<"false">> ->
            list(#{activated => false})
    end;

configs(delete, _Request) ->
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
    _ = emqx_mgmt:delete_all_deactivated_configs(),
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
    emqx_mgmt_api:select_table(Table, Ms, Start, Limit, fun format_config/1).

format_config(Alarms) when is_list(Alarms) ->
    [emqx_config:format(Alarm) || Alarm <- Alarms];

format_config(Alarm) ->
    emqx_config:format(Alarm).
