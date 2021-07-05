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
-module(emqx_mgmt_api_listeners).

-rest_api(v1).

%% API
-export([]).

-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_restart/1]).

rest_schema() ->
    DefinitionName = <<"listener">>,
    DefinitionProperties = #{
        <<"node">> => #{
            type => <<"string">>,
            description => <<"Node">>, example => node()},
        <<"acceptor">> => #{
            type => <<"integer">>,
            description => <<"Number of Acceptor proce">>},
        <<"liten_on">> => #{
            type => <<"string">>,
            description => <<"Litening port">>},
        <<"identifier">> => #{
            type => <<"string">>,
            description => <<"Identifier">>},
        <<"protocol">> => #{
            type => <<"string">>,
            description => <<"Plugin decription">>},
        <<"current_conn">> => #{
            type => <<"integer">>,
            description => <<"Whether plugin i enabled">>},
        <<"max_conn">> => #{
            type => <<"integer">>,
            description => <<"Maximum number of allowed connection">>},
        <<"shutdown_count">> => #{
            type => <<"string">>,
            description => <<"Reaon and count for connection hutdown">>}},
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [listeners_api(), restart_listeners_api()].

listeners_api() ->
    Metadata = #{
        get => #{
            tags => ["system"],
            description => "EMQ X listeners in cluster",
            operationId => handle_list,
            responses => #{
                <<"200">> => #{
                    description => <<"List clients 200 OK">>,
                    schema => #{
                        type => array,
                        items => cowboy_swagger:schema(<<"listener">>)}}}}},
    {"/listeners", Metadata}.

restart_listeners_api() ->
    Metadata = #{
        get => #{
            tags => ["system"],
            description => "EMQ X listeners in cluster",
            operationId => handle_restart,
            parameters => [#{
                name => listener_id,
                in => path,
                type => string,
                required => true,
                default => <<"mqtt:tcp:external">>
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Listener id not found">>),
                <<"200">> => #{description => <<"restart ok">>}}}},
    {"/listeners/:listener_id/restart", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(_Request) ->
    list(#{}).

handle_restart(Request) ->
    ListenerID = cowboy_req:binding(listener_id, Request),
    restart(#{listener_id => ListenerID}).

%%%==============================================================================================
%% api apply
list(_) ->
    Data = [format(Listeners) || Listeners <- emqx_mgmt:list_listeners()],
    Response = emqx_json:encode(lists:append(Data)),
    {ok, Response}.

restart(#{listener_id := ListenerID}) ->
    ArgsList = [[Node, ListenerID] || Node <- ekka_mnesia:running_nodes()],
    Data = emqx_mgmt_util:batch_operation(emqx_mgmt, restart_listener, ArgsList),
    Response = emqx_json:encode(Data),
    {ok, Response}.

format(Listeners) when is_list(Listeners) ->
    [ Info#{listen_on => list_to_binary(esockd:to_string(ListenOn))}
        || Info = #{listen_on := ListenOn} <- Listeners ];

format({error, Reason}) -> [{error, Reason}].
