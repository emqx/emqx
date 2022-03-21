%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_api).

-behaviour(minirest_api).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0, fields/1]).

-export([ lookup_retained_warp/2
        , with_topic_warp/2
        , config/2]).

-import(hoconsc, [mk/2, ref/1, ref/2, array/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-define(MAX_PAYLOAD_SIZE, 1048576). %% 1MB = 1024 x 1024
-define(PREFIX, "/mqtt/retainer").
-define(TAGS, [<<"retainer">>]).

namespace() -> "retainer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [?PREFIX, ?PREFIX ++ "/messages", ?PREFIX ++ "/message/:topic"].

schema(?PREFIX) ->
    #{'operationId' => config,
      get => #{tags => ?TAGS,
               description => <<"Get retainer config">>,
               responses => #{200 => mk(conf_schema(), #{desc => "The config content"}),
                              404 => error_codes(['NOT_FOUND'], <<"Config not found">>)
                             }
              },
      put => #{tags => ?TAGS,
               description => <<"Update retainer config">>,
               'requestBody' => mk(conf_schema(), #{desc => "The config content"}),
               responses => #{200 => mk(conf_schema(), #{desc => "Update configs successfully"}),
                              400 => error_codes(['UPDATE_FAILED'], <<"Update config failed">>)
                             }
              }
     };

schema(?PREFIX ++ "/messages") ->
    #{'operationId' => lookup_retained_warp,
      get => #{tags => ?TAGS,
               description => <<"List retained messages">>,
               parameters => page_params(),
               responses => #{200 => mk(array(ref(message_summary)), #{desc => "The result list"}),
                              400 => error_codes(['BAD_REQUEST'], <<"Unsupported backend">>)
                             }
              }
        };

schema(?PREFIX ++ "/message/:topic") ->
    #{'operationId' => with_topic_warp,
      get => #{tags => ?TAGS,
               description => <<"Lookup a message by a topic without wildcards">>,
               parameters => parameters(),
               responses => #{200 => mk(ref(message), #{desc => "Details of the message"}),
                              404 => error_codes(['NOT_FOUND'], <<"Viewed message doesn't exist">>),
                              400 => error_codes(['BAD_REQUEST'], <<"Unsupported backend">>)
                             }
              },
      delete => #{tags => ?TAGS,
                  description => <<"Delete matching messages">>,
                  parameters => parameters(),
                  responses => #{204 => <<>>,
                                 400 => error_codes(['BAD_REQUEST'],
                                                    <<"Unsupported backend">>)
                                }
                 }
     }.

page_params() ->
    emqx_dashboard_swagger:fields(page) ++ emqx_dashboard_swagger:fields(limit).

conf_schema() ->
    ref(emqx_retainer_schema, "retainer").

parameters() ->
    [{topic, mk(binary(), #{in => path,
                            required => true,
                            desc => "The target topic"
                           })}].

fields(message_summary) ->
    [ {msgid, mk(binary(), #{desc => <<"Message ID">>})}
    , {topic, mk(binary(), #{desc => "The topic"})}
    , {qos, mk(emqx_schema:qos(), #{desc => "The QoS"})}
    , {publish_at, mk(string(), #{desc => "Publish datetime, in RFC 3339 format"})}
    , {from_clientid, mk(binary(), #{desc => "Publisher ClientId"})}
    , {from_username, mk(binary(), #{desc => "Publisher Username"})}
    ];

fields(message) ->
    [{payload, mk(binary(), #{desc => "The payload content"})} |
     fields(message_summary)].

lookup_retained_warp(Type, Params) ->
    check_backend(Type, Params, fun lookup_retained/2).

with_topic_warp(Type, Params) ->
    check_backend(Type, Params, fun with_topic/2).

config(get, _) ->
    {200, emqx:get_raw_config([retainer])};

config(put, #{body := Body}) ->
    try
        {ok, _} = emqx_retainer:update_config(Body),
        {200, emqx:get_raw_config([retainer])}
    catch _:Reason:_ ->
            {400,
             #{code => <<"UPDATE_FAILED">>,
               message => iolist_to_binary(io_lib:format("~p~n", [Reason]))}}
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------
lookup_retained(get, #{query_string := Qs}) ->
    Page = maps:get(page, Qs, 1),
    Limit = maps:get(page, Qs, emqx_mgmt:max_row_limit()),
    {ok, Msgs} = emqx_retainer_mnesia:page_read(undefined, undefined, Page, Limit),
    {200, [format_message(Msg) || Msg <- Msgs]}.

with_topic(get, #{bindings := Bindings}) ->
    Topic = maps:get(topic, Bindings),
    {ok, Msgs} = emqx_retainer_mnesia:page_read(undefined, Topic, 1, 1),
    case Msgs of
        [H | _] ->
            {200, format_detail_message(H)};
        _ ->
            {404, #{code => <<"NOT_FOUND">>,
                    message => <<"Viewed message doesn't exist">>
                   }}
    end;

with_topic(delete, #{bindings := Bindings}) ->
    Topic = maps:get(topic, Bindings),
    emqx_retainer_mnesia:delete_message(undefined, Topic),
    {204}.

format_message(#message{ id = ID, qos = Qos, topic = Topic, from = From
                       , timestamp = Timestamp, headers = Headers}) ->
    #{msgid => emqx_guid:to_hexstr(ID),
      qos => Qos,
      topic => Topic,
      publish_at => list_to_binary(calendar:system_time_to_rfc3339(
                                     Timestamp, [{unit, millisecond}])),
      from_clientid => to_bin_string(From),
      from_username => maps:get(username, Headers, <<>>)
     }.

format_detail_message(#message{payload = Payload} = Msg) ->
    Base = format_message(Msg),
    case erlang:byte_size(Payload) =< ?MAX_PAYLOAD_SIZE of
        true ->
            Base#{payload => base64:encode(Payload)};
        _ ->
            Base
    end.

to_bin_string(Data) when is_binary(Data) ->
    Data;
to_bin_string(Data)  ->
    list_to_binary(io_lib:format("~p", [Data])).

check_backend(Type, Params, Cont) ->
    case emqx:get_config([retainer, backend, type]) of
        built_in_database ->
            Cont(Type, Params);
        _ ->
            {400, 'BAD_REQUEST', <<"This API only support built in database">>}
    end.
