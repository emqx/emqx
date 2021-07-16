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
-module(emqx_mgmt_api_stats).

-behavior(minirest_api).

-export([api_spec/0]).

-export([list/2]).

api_spec() ->
    {[stats_api()], [stats_schema()]}.

stats_schema() ->
    #{
        stats => #{
            type => object,
            properties => #{
                'connections.count' => #{
                    type => integer,
                    description => <<"Number of current connections">>},
                'connections.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of connections">>},
                'channels.count' => #{
                    type => integer,
                    description => <<"sessions.count">>},
                'channels.max' => #{
                    type => integer,
                    description => <<"session.max">>},
                'sessions.count' => #{
                    type => integer,
                    description => <<"Number of current sessions">>},
                'sessions.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of sessions">>},
                'topics.count' => #{
                    type => integer,
                    description => <<"Number of current topics">>},
                'topics.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of topics">>},
                'suboptions.count' => #{
                    type => integer,
                    description => <<"subscriptions.count">>},
                'suboptions.max' => #{
                    type => integer,
                    description => <<"subscriptions.max">>},
                'subscribers.count' => #{
                    type => integer,
                    description => <<"Number of current subscribers">>},
                'subscribers.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of subscribers">>},
                'subscriptions.count' => #{
                    type => integer,
                    description => <<"Number of current subscriptions, including shared subscriptions">>},
                'subscriptions.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of subscriptions">>},
                'subscriptions.shared.count' => #{
                    type => integer,
                    description => <<"Number of current shared subscriptions">>},
                'subscriptions.shared.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of shared subscriptions">>},
                'routes.count' => #{
                    type => integer,
                    description => <<"Number of current routes">>},
                'routes.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of routes">>},
                'retained.count' => #{
                    type => integer,
                    description => <<"Number of currently retained messages">>},
                'retained.max' => #{
                    type => integer,
                    description => <<"Historical maximum number of retained messages">>}
            }
        }
    }.

stats_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X stats",
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"List stats ok">>, <<"stats">>)}}},
    {"/stats", Metadata, list}.

%%%==============================================================================================
%% api apply
list(get, _Request) ->
    Response = emqx_json:encode(emqx_mgmt:get_stats()),
    {200, Response}.
