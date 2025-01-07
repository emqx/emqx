%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_publish).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(ALL_IS_WELL, 200).
-define(PARTIALLY_OK, 202).
-define(BAD_REQUEST, 400).
-define(DISPATCH_ERROR, 503).

-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    publish/2,
    publish_batch/2
]).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/publish", "/publish/bulk"].

schema("/publish") ->
    #{
        'operationId' => publish,
        post => #{
            summary => <<"Publish a message">>,
            description => ?DESC(publish_api),
            tags => [<<"Publish">>],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, publish_message)),
            responses => #{
                ?ALL_IS_WELL => hoconsc:mk(hoconsc:ref(?MODULE, publish_ok)),
                ?PARTIALLY_OK => hoconsc:mk(hoconsc:ref(?MODULE, publish_error)),
                ?BAD_REQUEST => hoconsc:mk(hoconsc:ref(?MODULE, bad_request)),
                ?DISPATCH_ERROR => hoconsc:mk(hoconsc:ref(?MODULE, publish_error))
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/publish/bulk") ->
    #{
        'operationId' => publish_batch,
        post => #{
            summary => <<"Publish a batch of messages">>,
            description => ?DESC(publish_bulk_api),
            tags => [<<"Publish">>],
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, publish_message)), #{}),
            responses => #{
                ?ALL_IS_WELL => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, publish_ok)), #{}),
                ?PARTIALLY_OK => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(?MODULE, publish_error)), #{}
                ),
                ?BAD_REQUEST => bad_request_schema(),
                ?DISPATCH_ERROR => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(?MODULE, publish_error)), #{}
                )
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    }.

bad_request_schema() ->
    Union = hoconsc:union([
        hoconsc:ref(?MODULE, bad_request),
        hoconsc:array(hoconsc:ref(?MODULE, publish_error))
    ]),
    hoconsc:mk(Union, #{}).

fields(message) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                desc => ?DESC(topic_name),
                required => true,
                example => <<"api/example/topic">>
            })},
        {qos,
            hoconsc:mk(emqx_schema:qos(), #{
                desc => ?DESC(qos),
                required => false,
                default => 0
            })},
        {clientid,
            hoconsc:mk(binary(), #{
                deprecated => {since, "v5.0.14"}
            })},
        {payload,
            hoconsc:mk(binary(), #{
                desc => ?DESC(payload),
                required => true,
                example => <<"hello emqx api">>
            })},
        {properties,
            hoconsc:mk(hoconsc:ref(?MODULE, message_properties), #{
                desc => ?DESC(message_properties),
                required => false
            })},
        {retain,
            hoconsc:mk(boolean(), #{
                desc => ?DESC(retain),
                required => false,
                default => false
            })}
    ];
fields(publish_message) ->
    [
        {payload_encoding,
            hoconsc:mk(hoconsc:enum([plain, base64]), #{
                desc => ?DESC(payload_encoding),
                required => false,
                default => plain
            })}
    ] ++ fields(message);
fields(message_properties) ->
    [
        {'payload_format_indicator',
            hoconsc:mk(typerefl:range(0, 1), #{
                desc => ?DESC(msg_payload_format_indicator),
                required => false,
                example => 0
            })},
        {'message_expiry_interval',
            hoconsc:mk(integer(), #{
                desc => ?DESC(msg_message_expiry_interval),
                required => false
            })},
        {'response_topic',
            hoconsc:mk(binary(), #{
                desc => ?DESC(msg_response_topic),
                required => false,
                example => <<"some_other_topic">>
            })},
        {'correlation_data',
            hoconsc:mk(binary(), #{
                desc => ?DESC(msg_correlation_data),
                required => false
            })},
        {'user_properties',
            hoconsc:mk(map(), #{
                desc => ?DESC(msg_user_properties),
                required => false,
                example => #{<<"foo">> => <<"bar">>}
            })},
        {'content_type',
            hoconsc:mk(binary(), #{
                desc => ?DESC(msg_content_type),
                required => false,
                example => <<"text/plain">>
            })}
    ];
fields(publish_ok) ->
    [
        {id,
            hoconsc:mk(binary(), #{
                desc => ?DESC(message_id)
            })}
    ];
fields(publish_error) ->
    [
        {reason_code,
            hoconsc:mk(integer(), #{
                desc => ?DESC(reason_code),
                example => 16
            })},
        {message,
            hoconsc:mk(binary(), #{
                desc => ?DESC(error_message),
                example => <<"no_matching_subscribers">>
            })}
    ];
fields(bad_request) ->
    [
        {code,
            hoconsc:mk(string(), #{
                desc => <<"BAD_REQUEST">>,
                example => ?RC_TOPIC_NAME_INVALID
            })},
        {message,
            hoconsc:mk(binary(), #{
                desc => ?DESC(error_message),
                example => to_binary(emqx_reason_codes:name(?RC_TOPIC_NAME_INVALID))
            })}
    ].

publish(post, #{body := Body}) ->
    case message(Body) of
        {ok, Message} ->
            Res = emqx_mgmt:publish(Message),
            publish_result_to_http_reply(Message, Res);
        {error, Reason} ->
            {?BAD_REQUEST, make_bad_req_reply(Reason)}
    end.

publish_batch(post, #{body := Body}) ->
    case messages(Body) of
        {ok, Messages} ->
            ResList = lists:map(
                fun(Message) ->
                    Res = emqx_mgmt:publish(Message),
                    publish_result_to_http_reply(Message, Res)
                end,
                Messages
            ),
            publish_results_to_http_reply(ResList);
        {error, Reason} ->
            {?BAD_REQUEST, make_bad_req_reply(Reason)}
    end.

make_bad_req_reply(invalid_topic_name) ->
    make_publish_error_response(?RC_TOPIC_NAME_INVALID);
make_bad_req_reply(packet_too_large) ->
    %% 0x95 RC_PACKET_TOO_LARGE is not a PUBACK reason code
    %% This is why we use RC_QUOTA_EXCEEDED instead
    make_publish_error_response(?RC_QUOTA_EXCEEDED, packet_too_large);
make_bad_req_reply(Reason) ->
    make_publish_error_response(?RC_IMPLEMENTATION_SPECIFIC_ERROR, to_binary(Reason)).

-spec is_ok_deliver({_NodeOrShare, _MatchedTopic, emqx_types:deliver_result()}) -> boolean().
is_ok_deliver({_NodeOrShare, _MatchedTopic, ok}) -> true;
is_ok_deliver({_NodeOrShare, _MatchedTopic, {ok, _}}) -> true;
is_ok_deliver(persisted) -> true;
is_ok_deliver({_NodeOrShare, _MatchedTopic, {error, _}}) -> false.

%% @hidden Map MQTT publish result reason code to HTTP status code.
%% MQTT reason code | Description                           | HTTP status code
%% 0                  Success                                 200
%% 16                 No matching subscribers                 202
%% 128                Unspecified error                       406
%% 131                Implementation specific error           406
%% 144                Topic Name invalid                      400
%% 151                Quota exceeded                          400
%%
%% %%%%%% Below error codes are not implemented so far %%%%
%%
%% If HTTP request passes HTTP authentication, it is considered trusted.
%% 135                Not authorized                          401
%%
%% %%%%%% Below error codes are not applicable %%%%%%%
%%
%% No user specified packet ID, so there should be no packet ID error
%% 145                Packet identifier is in use             400
%%
%% No preceding payload format indicator to compare against.
%% Content-Type check should be done at HTTP layer but not here.
%% 153                Payload format invalid                  400
publish_result_to_http_reply(#message{topic = <<"$delayed/", _/binary>>} = Message, []) ->
    {?ALL_IS_WELL, make_publish_response(Message)};
publish_result_to_http_reply(_Message, []) ->
    %% matched no subscriber
    {?PARTIALLY_OK, make_publish_error_response(?RC_NO_MATCHING_SUBSCRIBERS)};
publish_result_to_http_reply(Message, PublishResult) ->
    case lists:any(fun is_ok_deliver/1, PublishResult) of
        true ->
            %% delivered to at least one subscriber
            OkBody = make_publish_response(Message),
            {?ALL_IS_WELL, OkBody};
        false ->
            %% this is quite unusual, matched, but failed to deliver
            %% if this happens, the publish result log can be helpful
            %% to idnetify the reason why publish failed
            %% e.g. during emqx restart
            ReasonString = <<"failed_to_dispatch">>,
            ErrorBody = make_publish_error_response(
                ?RC_IMPLEMENTATION_SPECIFIC_ERROR, ReasonString
            ),
            ?SLOG(warning, #{
                msg => ReasonString,
                message_id => emqx_message:id(Message),
                results => PublishResult
            }),
            {?DISPATCH_ERROR, ErrorBody}
    end.

%% @hidden Reply batch publish result.
%% 200 if all published OK.
%% 202 if at least one message matched no subscribers.
%% 503 for temp errors duing EMQX restart
publish_results_to_http_reply([_ | _] = ResList) ->
    {Codes0, BodyL} = lists:unzip(ResList),
    Codes = lists:usort(Codes0),
    HasFailure = lists:member(?DISPATCH_ERROR, Codes),
    All200 = (Codes =:= [?ALL_IS_WELL]),
    Code =
        case All200 of
            true ->
                %% All OK
                ?ALL_IS_WELL;
            false when not HasFailure ->
                %% Partially OK
                ?PARTIALLY_OK;
            false ->
                %% At least one failed
                ?DISPATCH_ERROR
        end,
    {Code, BodyL}.

message(Map) ->
    try
        make_message(Map)
    catch
        throw:Reason ->
            {error, Reason}
    end.

make_message(Map) ->
    Encoding = maps:get(<<"payload_encoding">>, Map, plain),
    case decode_payload(Encoding, maps:get(<<"payload">>, Map)) of
        {ok, Payload} ->
            QoS = maps:get(<<"qos">>, Map, 0),
            Topic = maps:get(<<"topic">>, Map),
            Retain = maps:get(<<"retain">>, Map, false),
            Headers =
                case maps:get(<<"properties">>, Map, #{}) of
                    Properties when
                        is_map(Properties) andalso
                            map_size(Properties) > 0
                    ->
                        #{properties => to_msg_properties(Properties)};
                    _ ->
                        #{}
                end,
            try
                _ = emqx_topic:validate(name, Topic)
            catch
                error:_Reason ->
                    throw(invalid_topic_name)
            end,
            Message = emqx_message:make(
                http_api, QoS, Topic, Payload, #{retain => Retain}, Headers
            ),
            Size = emqx_message:estimate_size(Message),
            (Size > size_limit()) andalso throw(packet_too_large),
            {ok, Message};
        {error, R} ->
            {error, R}
    end.

to_msg_properties(Properties) ->
    maps:fold(
        fun to_property/3,
        #{},
        Properties
    ).

to_property(<<"payload_format_indicator">>, V, M) -> M#{'Payload-Format-Indicator' => V};
to_property(<<"message_expiry_interval">>, V, M) -> M#{'Message-Expiry-Interval' => V};
to_property(<<"response_topic">>, V, M) -> M#{'Response-Topic' => V};
to_property(<<"correlation_data">>, V, M) -> M#{'Correlation-Data' => V};
to_property(<<"user_properties">>, V, M) -> M#{'User-Property' => maps:to_list(V)};
to_property(<<"content_type">>, V, M) -> M#{'Content-Type' => V}.

%% get the global packet size limit since HTTP API does not belong to any zone.
size_limit() ->
    try
        emqx_config:get([mqtt, max_packet_size])
    catch
        _:_ ->
            %% leave 1000 bytes for topic name etc.
            ?MAX_PACKET_SIZE
    end.

decode_payload(plain, Payload) ->
    {ok, Payload};
decode_payload(base64, Payload) ->
    try
        {ok, base64:decode(Payload)}
    catch
        _:_ ->
            {error, {decode_base64_payload_failed, Payload}}
    end.

messages([]) ->
    {errror, <<"empty_batch">>};
messages(List) ->
    messages(List, []).

messages([], Res) ->
    {ok, lists:reverse(Res)};
messages([MessageMap | List], Res) ->
    case message(MessageMap) of
        {ok, Message} ->
            messages(List, [Message | Res]);
        {error, R} ->
            {error, R}
    end.

make_publish_response(#message{id = ID}) ->
    #{
        id => emqx_guid:to_hexstr(ID)
    }.

make_publish_error_response(ReasonCode) ->
    make_publish_error_response(ReasonCode, emqx_reason_codes:name(ReasonCode)).

make_publish_error_response(ReasonCode, Msg) ->
    #{
        reason_code => ReasonCode,
        message => to_binary(Msg)
    }.

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom);
to_binary(Msg) when is_binary(Msg) ->
    Msg;
to_binary(Term) ->
    list_to_binary(io_lib:format("~0p", [Term])).
