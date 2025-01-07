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

-module(emqx_delayed_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_modules.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).

-export([
    status/2,
    delayed_messages/2,
    delayed_message/2,
    delayed_message_topic/2
]).

-export([
    paths/0,
    fields/1,
    schema/1,
    namespace/0
]).

%% for rpc
-export([update_config_/1]).

-export([api_spec/0]).

-define(MAX_PAYLOAD_LENGTH, 2048).
-define(PAYLOAD_TOO_LARGE, <<"PAYLOAD_TOO_LARGE">>).

-define(INTERNAL_ERROR, 'INTERNAL_ERROR').
-define(BAD_REQUEST, 'BAD_REQUEST').

-define(MESSAGE_ID_NOT_FOUND, 'MESSAGE_ID_NOT_FOUND').
-define(MESSAGE_ID_SCHEMA_ERROR, 'MESSAGE_ID_SCHEMA_ERROR').
-define(INVALID_NODE, 'INVALID_NODE').

-define(INVALID_TOPIC, 'INVALID_TOPIC_NAME').
-define(MESSAGE_NOT_FOUND, 'MESSAGE_NOT_FOUND').

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/mqtt/delayed",
        "/mqtt/delayed/messages",
        "/mqtt/delayed/messages/:topic",
        "/mqtt/delayed/messages/:node/:msgid"
    ].

schema("/mqtt/delayed") ->
    #{
        'operationId' => status,
        get => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(view_status_api),
            responses => #{
                200 => ref(emqx_modules_schema, "delayed")
            }
        },
        put => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(update_api),
            'requestBody' => ref(emqx_modules_schema, "delayed"),
            responses => #{
                200 => mk(
                    ref(emqx_modules_schema, "delayed"),
                    #{desc => ?DESC(update_success)}
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST],
                    ?DESC(illegality_limit)
                )
            }
        }
    };
schema("/mqtt/delayed/messages/:topic") ->
    #{
        'operationId' => delayed_message_topic,
        delete => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(delete_api),
            parameters => [
                {topic,
                    mk(
                        binary(),
                        #{in => path, desc => ?DESC(topic)}
                    )}
            ],
            responses => #{
                204 => <<"Delete delayed message success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?INVALID_TOPIC],
                    ?DESC(bad_topic_name)
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    [?MESSAGE_NOT_FOUND],
                    ?DESC(no_delayed_message)
                )
            }
        }
    };
schema("/mqtt/delayed/messages/:node/:msgid") ->
    #{
        'operationId' => delayed_message,
        get => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(get_message_api),
            parameters => [
                {node,
                    mk(
                        binary(),
                        #{in => path, desc => ?DESC(node)}
                    )},
                {msgid, mk(binary(), #{in => path, desc => ?DESC(msgid)})}
            ],
            responses => #{
                200 => ref("message_without_payload"),
                400 => emqx_dashboard_swagger:error_codes(
                    [?MESSAGE_ID_SCHEMA_ERROR, ?INVALID_NODE],
                    ?DESC(bad_msgid_format)
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    [?MESSAGE_ID_NOT_FOUND],
                    ?DESC(msgid_not_found)
                )
            }
        },
        delete => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(delete_api),
            parameters => [
                {node,
                    mk(
                        binary(),
                        #{in => path, desc => ?DESC(node)}
                    )},
                {msgid, mk(binary(), #{in => path, desc => ?DESC(msgid)})}
            ],
            responses => #{
                204 => <<"Delete delayed message success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?MESSAGE_ID_SCHEMA_ERROR, ?INVALID_NODE],
                    ?DESC(bad_msgid_format)
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    [?MESSAGE_ID_NOT_FOUND],
                    ?DESC(msgid_not_found)
                )
            }
        }
    };
schema("/mqtt/delayed/messages") ->
    #{
        'operationId' => delayed_messages,
        get => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(list_api),
            parameters => [ref(emqx_dashboard_swagger, page), ref(emqx_dashboard_swagger, limit)],
            responses => #{
                200 =>
                    [
                        {data, mk(hoconsc:array(ref("message")), #{})},
                        {meta, [
                            {page, mk(pos_integer(), #{desc => ?DESC(view_page)})},
                            {limit, mk(pos_integer(), #{desc => ?DESC(view_limit)})},
                            {count, mk(non_neg_integer(), #{desc => ?DESC(count)})}
                        ]}
                    ]
            }
        }
    }.

fields("message_without_payload") ->
    [
        {msgid, mk(integer(), #{desc => ?DESC(msgid)})},
        {node, mk(binary(), #{desc => ?DESC(node)})},
        {publish_at, mk(binary(), #{desc => ?DESC(publish_at)})},
        {delayed_interval, mk(pos_integer(), #{desc => ?DESC(delayed_interval)})},
        {delayed_remaining, mk(non_neg_integer(), #{desc => ?DESC(delayed_remaining)})},
        {expected_at, mk(binary(), #{desc => ?DESC(expected_at)})},
        {topic, mk(binary(), #{desc => ?DESC(topic), example => <<"/sys/#">>})},
        {qos, mk(emqx_schema:qos(), #{desc => ?DESC(qos)})},
        {from_clientid, mk(binary(), #{desc => ?DESC(from_clientid)})},
        {from_username, mk(binary(), #{desc => ?DESC(from_username)})}
    ];
fields("message") ->
    fields("message_without_payload") ++
        [{payload, mk(binary(), #{desc => ?DESC(payload)})}].

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Params) ->
    {200, get_status()};
status(put, #{body := Body}) ->
    update_config(Body).

delayed_messages(get, #{query_string := Qs}) ->
    {200, emqx_delayed:cluster_list(Qs)}.

delayed_message(get, #{bindings := #{node := NodeBin, msgid := HexId}}) ->
    MaybeNode = make_maybe(NodeBin, invalid_node, fun erlang:binary_to_existing_atom/1),
    MaybeId = make_maybe(HexId, id_schema_error, fun emqx_guid:from_hexstr/1),
    with_maybe(
        [MaybeNode, MaybeId],
        fun(Node, Id) ->
            case emqx_delayed:get_delayed_message(Node, Id) of
                {ok, Message} ->
                    Payload = maps:get(payload, Message),
                    case erlang:byte_size(Payload) > ?MAX_PAYLOAD_LENGTH of
                        true ->
                            {200, Message#{payload => ?PAYLOAD_TOO_LARGE}};
                        _ ->
                            {200, Message#{payload => base64:encode(Payload)}}
                    end;
                {error, not_found} ->
                    {404, generate_http_code_map(not_found, HexId)};
                {badrpc, _} ->
                    {400, generate_http_code_map(invalid_node, NodeBin)}
            end
        end
    );
delayed_message(delete, #{bindings := #{node := NodeBin, msgid := HexId}}) ->
    MaybeNode = make_maybe(NodeBin, invalid_node, fun erlang:binary_to_atom/1),
    MaybeId = make_maybe(HexId, id_schema_error, fun emqx_guid:from_hexstr/1),
    with_maybe(
        [MaybeNode, MaybeId],
        fun(Node, Id) ->
            case emqx_delayed:delete_delayed_message(Node, Id) of
                ok ->
                    {204};
                {error, not_found} ->
                    {404, generate_http_code_map(not_found, Id)}
            end
        end
    ).

delayed_message_topic(delete, #{bindings := #{topic := Topic}}) ->
    MaybeTopic = make_maybe(Topic, invalid_topic_name, fun validate_topic_name/1),
    with_maybe(
        [MaybeTopic],
        fun(TopicName) ->
            case emqx_delayed:delete_delayed_messages_by_topic_name(TopicName) of
                ok ->
                    {204};
                {error, not_found} ->
                    {404, generate_http_code_map(message_not_found, TopicName)}
            end
        end
    ).
%%--------------------------------------------------------------------
%% internal function
%%--------------------------------------------------------------------
get_status() ->
    emqx_conf:get([delayed], #{}).

update_config(Config) ->
    case generate_config(Config) of
        {ok, Config} ->
            update_config_(Config);
        {error, {Code, Message}} ->
            {400, #{code => Code, message => Message}}
    end.
generate_config(Config) ->
    generate_config(Config, [fun generate_max_delayed_messages/1]).

generate_config(Config, []) ->
    {ok, Config};
generate_config(Config, [Fun | Tail]) ->
    case Fun(Config) of
        {ok, Config} ->
            generate_config(Config, Tail);
        {error, CodeMessage} ->
            {error, CodeMessage}
    end.

generate_max_delayed_messages(Config = #{<<"max_delayed_messages">> := Max}) when Max >= 0 ->
    {ok, Config};
generate_max_delayed_messages(#{<<"max_delayed_messages">> := Max}) when Max < 0 ->
    {error, {?BAD_REQUEST, <<"Max delayed must be equal or greater than 0">>}};
generate_max_delayed_messages(Config) ->
    {ok, Config}.

update_config_(Config) ->
    case emqx_delayed:update_config(Config) of
        {ok, #{raw_config := NewDelayed}} ->
            {200, NewDelayed};
        {error, Reason} ->
            Message = list_to_binary(
                io_lib:format("Update config failed ~p", [Reason])
            ),
            {500, ?INTERNAL_ERROR, Message}
    end.

generate_http_code_map(id_schema_error, Id) ->
    #{
        code => ?MESSAGE_ID_SCHEMA_ERROR,
        message =>
            iolist_to_binary(io_lib:format("Message ID ~s schema error", [Id]))
    };
generate_http_code_map(not_found, Id) ->
    #{
        code => ?MESSAGE_ID_NOT_FOUND,
        message =>
            iolist_to_binary(io_lib:format("Message ID ~s not found", [Id]))
    };
generate_http_code_map(message_not_found, Topic) ->
    #{
        code => ?MESSAGE_NOT_FOUND,
        message =>
            iolist_to_binary(io_lib:format("Not found messages for ~s", [Topic]))
    };
generate_http_code_map(invalid_topic_name, Topic) ->
    #{
        code => ?INVALID_TOPIC,
        message =>
            iolist_to_binary(io_lib:format("The topic name ~s is invalid", [Topic]))
    };
generate_http_code_map(invalid_node, Node) ->
    #{
        code => ?INVALID_NODE,
        message =>
            iolist_to_binary(io_lib:format("The node name ~s is invalid", [Node]))
    }.

make_maybe(X, Error, Fun) ->
    try Fun(X) of
        Right ->
            Right
    catch
        _:_ ->
            {left, X, Error}
    end.

validate_topic_name(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            error(badarg);
        false ->
            Topic
    end.

with_maybe(Maybes, Cont) ->
    with_maybe(Maybes, Cont, []).

with_maybe([], Cont, Rights) ->
    erlang:apply(Cont, lists:reverse(Rights));
with_maybe([{left, X, Error} | _], _Cont, _Rights) ->
    {400, generate_http_code_map(Error, X)};
with_maybe([Right | T], Cont, Rights) ->
    with_maybe(T, Cont, [Right | Rights]).
