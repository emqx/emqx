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
-module(emqx_mgmt_api_routes).

-include_lib("emqx/include/emqx.hrl").

%% API
-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_get/1]).

rest_schema() ->
    DefinitionName = <<"routs">>,
    DefinitionProperties = #{
        <<"topic">> =>
            #{
                type => <<"string">>
            },
        <<"node">> =>
            #{
                type => <<"string">>,
                example => node()
            }
    },
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [routs_api(), rout_api()].

routs_api() ->
    Metadata = #{
        get =>
            #{tags => ["system"],
            description => "EMQ X routs",
            operationId => handle_list,
            parameters =>
                [#{
                    name => page,
                    in => query,
                    description => <<"Page">>,
                    required => true,
                    schema => #{type => integer, default => 1}
                },
                #{name => limit
                    , in => query
                    , description => <<"Page size">>
                    , required => true
                    , schema => #{type => integer, default => emqx_mgmt:max_row_limit()}
                }],
            responses => #{
                <<"200">> => #{content => #{'application/json' =>
                        #{schema => cowboy_swagger:schema(<<"routs">>)}}}}}},
    {"/routs", Metadata}.

rout_api() ->
    Metadata = #{
        get =>
            #{tags => ["system"],
            description => "EMQ X routs",
            operationId => handle_get,
            parameters =>
                [#{
                    name => topic,
                    in => path,
                    description => <<"topic">>,
                    required => true,
                    schema => #{type => string}
                }],
            responses => #{
                <<"200">> => #{content => #{'application/json' =>
                        #{schema => cowboy_swagger:schema(<<"routs">>)}}}}}},
    {"/routs/:topic", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(Request) ->
    Params = cowboy_req:parse_qs(Request),
    list(Params).

handle_get(Request) ->
    Topic = cowboy_req:binding(topic, Request),
    lookup(#{topic => Topic}).

%%%==============================================================================================
%% api apply
list(Params) ->
    Data = emqx_mgmt_api:paginate(emqx_route, Params, fun format/1),
    Response = emqx_json:encode(Data),
    {200, Response}.

lookup(#{topic := Topic}) ->
    Data = [format(R) || R <- emqx_mgmt:lookup_routes(Topic)],
    Response = emqx_json:encode(Data),
    {200, Response}.

%%%==============================================================================================
%% internal
format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.
