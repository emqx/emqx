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

-module(emqx_retainer_api).

-behaviour(minirest_api).

-include_lib("emqx/include/emqx.hrl").

-export([api_spec/0]).

-export([ lookup_retained_warp/2
        , with_topic_warp/2
        , config/2]).

-import(emqx_mgmt_api_configs, [gen_schema/1]).
-import(emqx_mgmt_util, [ object_array_schema/2
                        , schema/1
                        , schema/2
                        , error_schema/2
                        , page_params/0
                        , properties/1]).

api_spec() ->
    {
     [ lookup_retained_api()
     , with_topic_api()
     , config_api()
     ],
     schemas()
    }.

schemas() ->
    MqttRetainer = gen_schema(emqx:get_raw_config([emqx_retainer])),
    [#{emqx_retainer => MqttRetainer}].

message_props() ->
    properties([
        {id, string, <<"Message ID">>},
        {topic, string, <<"MQTT Topic">>},
        {qos, string, <<"MQTT QoS">>},
        {payload, string, <<"MQTT Payload">>},
        {publish_at, string, <<"publish datetime">>},
        {from_clientid, string, <<"publisher ClientId">>},
        {from_username, string, <<"publisher Username">>}
        ]).

parameters() ->
    [#{
        name => topic,
        in => path,
        required => true,
        schema => #{type => "string"}
    }].

lookup_retained_api() ->
    Metadata = #{
        get => #{
            description => <<"lookup matching messages">>,
            parameters => page_params(),
            responses => #{
                <<"200">> => object_array_schema(
                    maps:without([payload], message_props()),
                    <<"List retained messages">>),
                <<"405">> => schema(<<"NotAllowed">>)
            }
        }
    },
    {"/mqtt/retainer/messages", Metadata, lookup_retained_warp}.

with_topic_api() ->
    MetaData = #{
        get => #{
            description => <<"lookup matching messages">>,
            parameters => parameters() ++ page_params(),
            responses => #{
                <<"200">> => object_array_schema(message_props(), <<"List retained messages">>),
                <<"405">> => schema(<<"NotAllowed">>)
            }
        },
        delete => #{
            description => <<"delete matching messages">>,
            parameters => parameters(),
            responses => #{
                <<"200">> => schema(<<"Successed">>),
                <<"405">> => schema(<<"NotAllowed">>)
            }
        }
    },
    {"/mqtt/retainer/message/:topic", MetaData, with_topic_warp}.

config_api() ->
    MetaData = #{
        get => #{
            description => <<"get retainer config">>,
            responses => #{
                <<"200">> => schema(mqtt_retainer, <<"Get configs successfully">>),
                <<"404">> => error_schema(<<"Config not found">>, ['NOT_FOUND'])
            }
        },
        put => #{
            description => <<"Update retainer config">>,
            'requestBody' => schema(mqtt_retainer),
            responses => #{
                <<"200">> => schema(mqtt_retainer, <<"Update configs successfully">>),
                <<"400">> => error_schema(<<"Update configs failed">>, ['UPDATE_FAILED'])
            }
        }
    },
    {"/mqtt/retainer", MetaData, config}.

lookup_retained_warp(Type, Req) ->
    check_backend(Type, Req, fun lookup_retained/2).

with_topic_warp(Type, Req) ->
    check_backend(Type, Req, fun with_topic/2).

config(get, _) ->
    Config = emqx:get_config([mqtt_retainer]),
    Body = emqx_json:encode(Config),
    {200, Body};

config(put, Req) ->
    try
        {ok, Body, _} = cowboy_req:read_body(Req),
        Cfg = emqx_json:decode(Body),
        {ok, RawConf} = hocon:binary(jsx:encode(#{<<"mqtt_retainer">> => Cfg}),
                                     #{format => richmap}),
        RichConf = hocon_schema:check(emqx_retainer_schema, RawConf, #{atom_key => true}),
        #{mqtt_retainer := Conf} = hocon_schema:richmap_to_map(RichConf),
        emqx_retainer:update_config(Conf),
        {200,  #{<<"content-type">> => <<"text/plain">>}, <<"Update configs successfully">>}
    catch _:Reason:_ ->
            {400,
             #{code => 'UPDATE_FAILED',
               message => iolist_to_binary(io_lib:format("~p~n", [Reason]))}}
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------
lookup_retained(get, Req) ->
    lookup(undefined, Req, fun format_message/1).

with_topic(get, Req) ->
    Topic = cowboy_req:binding(topic, Req),
    lookup(Topic, Req, fun format_detail_message/1);

with_topic(delete, Req) ->
    Topic = cowboy_req:binding(topic, Req),
    emqx_retainer_mnesia:delete_message(undefined, Topic),
    {200}.

-spec lookup(undefined | binary(),
             cowboy_req:req(),
             fun((#message{}) -> map())) ->
          {200, map()}.
lookup(Topic, Req, Formatter) ->
    #{page := Page,
      limit := Limit} = cowboy_req:match_qs([{page, int, 1},
                                             {limit, int, emqx_mgmt:max_row_limit()}],
                                            Req),
    {ok, Msgs} = emqx_retainer_mnesia:page_read(undefined, Topic, Page, Limit),
    {200, format_message(Msgs, Formatter)}.


format_message(Messages, Formatter) when is_list(Messages)->
    [Formatter(Message) || Message <- Messages];

format_message(Message, Formatter) ->
    Formatter(Message).

format_message(#message{id = ID, qos = Qos, topic = Topic, from = From, timestamp = Timestamp, headers = Headers}) ->
    #{msgid => emqx_guid:to_hexstr(ID),
      qos => Qos,
      topic => Topic,
      publish_at => erlang:list_to_binary(emqx_mgmt_util:strftime(Timestamp div 1000)),
      from_clientid => to_bin_string(From),
      from_username => maps:get(username, Headers, <<>>)
     }.

format_detail_message(#message{payload = Payload} = Msg) ->
    Base = format_message(Msg),
    Base#{payload => Payload}.

to_bin_string(Data) when is_binary(Data) ->
    Data;
to_bin_string(Data)  ->
    list_to_binary(io_lib:format("~p", [Data])).

check_backend(Type, Req, Cont) ->
    case emqx:get_config([emqx_retainer, config, type]) of
        built_in_database ->
            Cont(Type, Req);
        _ ->
            {405,
             #{<<"content-type">> => <<"text/plain">>},
             <<"This API only for built in database">>}
    end.
