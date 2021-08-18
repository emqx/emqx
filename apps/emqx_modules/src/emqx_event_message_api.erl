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
-module(emqx_event_message_api).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([event_message/2]).


api_spec() ->
    {[event_message_api()], [event_message_schema()]}.

event_message_schema() ->
    #{
        type => object,
        properties => #{
            '$event/client_connected' => #{
                type => boolean,
                description => <<"Client connected event">>,
                example => get_raw(<<"$event/client_connected">>)
            },
            '$event/client_disconnected' => #{
                type => boolean,
                description => <<"client_disconnected">>,
                example => get_raw(<<"Client disconnected event">>)
            },
            '$event/client_subscribed' => #{
                type => boolean,
                description => <<"client_subscribed">>,
                example => get_raw(<<"Client subscribed event">>)
            },
            '$event/client_unsubscribed' => #{
                type => boolean,
                description => <<"client_unsubscribed">>,
                example => get_raw(<<"Client unsubscribed event">>)
            },
            '$event/message_delivered' => #{
                type => boolean,
                description => <<"message_delivered">>,
                example => get_raw(<<"Message delivered event">>)
            },
            '$event/message_acked' => #{
                type => boolean,
                description => <<"message_acked">>,
                example => get_raw(<<"Message acked event">>)
            },
            '$event/message_dropped' => #{
                type => boolean,
                description => <<"message_dropped">>,
                example => get_raw(<<"Message dropped event">>)
            }
        }
    }.

event_message_api() ->
    Path = "/mqtt/event_message",
    Metadata = #{
        get => #{
            description => <<"Event Message">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<>>, event_message_schema())}},
        post => #{
            description => <<"">>,
            'requestBody' => emqx_mgmt_util:request_body_schema(event_message_schema()),
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<>>, event_message_schema())
            }
        }
    },
    {Path, Metadata, event_message}.

event_message(get, _Request) ->
    {200, emqx_event_message:list()};

event_message(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    _ = emqx_event_message:update(Params),
    {200, emqx_event_message:list()}.

get_raw(Key) ->
    emqx_config:get_raw([<<"event_message">>] ++ [Key], false).
