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

%% API
-export([rest_schema/0, rest_api/0]).

-export([handle_list/1, handle_delete/1]).

rest_schema() ->
    DefinitionName = <<"alarm">>,
    DefinitionProperties = #{
        <<"node">> =>
        #{type => <<"string">>, description => <<"Alarm in node">>},
        <<"name">> =>
        #{type => <<"string">>, description => <<"Alarm name">>},
        <<"message">> =>
        #{type => <<"string">>, description => <<"Alarm readable information">>},
        <<"details">> =>
        #{type => <<"object">>, description => <<"Alarm detail">>},
        <<"activate_at">> =>
        #{type => <<"integer">>, description => <<"Alarms activated time UNIX time stamp">>},
        <<"deactivate_at">> =>
        #{type => <<"integer">>, description => <<"Alarms deactivated time UNIX time stamp">>},
        <<"activated">> =>
        #{type => <<"boolean">>, description => <<"Activated or deactivated">>}
    },
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [alarms_api()].

alarms_api() ->
    Metadata = #{
        get =>
        #{tags => ["monitoring"],
            description => "EMQ X alarms",
            operationId => handle_list,
            parameters => [
                #{name => activated
                , in => query
                , description => <<"All alarms, if not specified">>
                , required => false
                , schema =>
                    #{type => boolean, example => false}
            }],
            responses => #{
                <<"200">> => #{
                    content => #{
                        'application/json' =>
                        #{schema =>
                        #{type => array,
                            items => cowboy_swagger:schema(<<"alarm">>)}}}}}},
        delete =>
        #{tags => ["monitoring"],
            description => "Remove all deactivated alarms",
            operationId => handle_delete,
            responses => #{
                <<"200">> => #{description => <<"Deactivated alarms removed">>}}}
    },
    {"/alarms", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(Request) ->
    case proplists:get_value(<<"activated">>, cowboy_req:parse_qs(Request), undefined) of
        undefined ->
            list(#{activated => undefined});
        <<"true">> ->
            list(#{activated => true});
        <<"false">> ->
            list(#{activated => false})
    end.

handle_delete(_Request) ->
    delete(#{}).

%%%==============================================================================================
%% api apply
list(#{activated := true}) ->
    do_list(activated);
list(#{activated := false}) ->
    do_list(deactivated);
list(#{activated := undefined}) ->
    do_list(all).

delete(_) ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {ok}.

%%%==============================================================================================
%% internal
do_list(Type) ->
    Fun =
        fun({Node, NodeAlarms}, Result) ->
            lists:append(Result, [maps:put(node, Node, NodeAlarm) || NodeAlarm <- NodeAlarms])
        end,
    Alarms = lists:foldl(Fun, [], emqx_mgmt:get_alarms(Type)),
    Response = emqx_json:encode(Alarms),
    {ok, Response}.
