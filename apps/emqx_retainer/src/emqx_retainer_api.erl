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

-include("emqx_retainer.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0, fields/1]).

-export([
    lookup_retained_warp/2,
    with_topic_warp/2,
    config/2
]).

-import(hoconsc, [mk/1, mk/2, ref/1, ref/2, array/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

%% 1MB = 1024 x 1024
-define(MAX_PAYLOAD_SIZE, 1048576).
-define(PREFIX, "/mqtt/retainer").
-define(TAGS, [<<"retainer">>]).

namespace() -> "retainer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [?PREFIX, ?PREFIX ++ "/messages", ?PREFIX ++ "/message/:topic"].

schema(?PREFIX) ->
    #{
        'operationId' => config,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_config_api),
            responses => #{
                200 => mk(conf_schema(), #{desc => ?DESC(config_content)}),
                404 => error_codes(['NOT_FOUND'], ?DESC(config_not_found))
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(update_retainer_api),
            'requestBody' => mk(conf_schema(), #{desc => ?DESC(config_content)}),
            responses => #{
                200 => mk(conf_schema(), #{desc => ?DESC(update_config_success)}),
                400 => error_codes(['UPDATE_FAILED'], ?DESC(update_config_failed))
            }
        }
    };
schema(?PREFIX ++ "/messages") ->
    #{
        'operationId' => lookup_retained_warp,
        get => #{
            tags => ?TAGS,
            description => ?DESC(list_retained_api),
            parameters => page_params(),
            responses => #{
                200 => [
                    {data, mk(array(ref(message_summary)), #{desc => ?DESC(retained_list)})},
                    {meta, mk(hoconsc:ref(emqx_dashboard_swagger, meta))}
                ],
                400 => error_codes(['BAD_REQUEST'], ?DESC(unsupported_backend))
            }
        }
    };
schema(?PREFIX ++ "/message/:topic") ->
    #{
        'operationId' => with_topic_warp,
        get => #{
            tags => ?TAGS,
            description => ?DESC(lookup_api),
            parameters => parameters(),
            responses => #{
                200 => mk(ref(message), #{desc => ?DESC(message_detail)}),
                404 => error_codes(['NOT_FOUND'], ?DESC(message_not_exist)),
                400 => error_codes(['BAD_REQUEST'], ?DESC(unsupported_backend))
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(delete_matching_api),
            parameters => parameters(),
            responses => #{
                204 => <<>>,
                400 => error_codes(
                    ['BAD_REQUEST'],
                    ?DESC(unsupported_backend)
                )
            }
        }
    }.

page_params() ->
    emqx_dashboard_swagger:fields(page) ++ emqx_dashboard_swagger:fields(limit).

conf_schema() ->
    ref(emqx_retainer_schema, "retainer").

parameters() ->
    [
        {topic,
            mk(binary(), #{
                in => path,
                required => true,
                desc => ?DESC(topic)
            })}
    ].

fields(message_summary) ->
    [
        {msgid, mk(binary(), #{desc => ?DESC(msgid)})},
        {topic, mk(binary(), #{desc => ?DESC(topic)})},
        {qos, mk(emqx_schema:qos(), #{desc => ?DESC(qos)})},
        {publish_at, mk(string(), #{desc => ?DESC(publish_at)})},
        {from_clientid, mk(binary(), #{desc => ?DESC(from_clientid)})},
        {from_username, mk(binary(), #{desc => ?DESC(from_username)})}
    ];
fields(message) ->
    [
        {payload, mk(binary(), #{desc => ?DESC(payload)})}
        | fields(message_summary)
    ].

lookup_retained_warp(Type, Params) ->
    check_backend(Type, Params, fun lookup_retained/2).

with_topic_warp(Type, Params) ->
    check_backend(Type, Params, fun with_topic/2).

config(get, _) ->
    {200, emqx:get_raw_config([retainer])};
config(put, #{body := Body}) ->
    try
        check_bucket_exists(
            Body,
            fun(Conf) ->
                {ok, _} = emqx_retainer:update_config(Conf),
                {200, emqx:get_raw_config([retainer])}
            end
        )
    catch
        _:Reason:_ ->
            {400, #{
                code => <<"UPDATE_FAILED">>,
                message => iolist_to_binary(io_lib:format("~p~n", [Reason]))
            }}
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------
lookup_retained(get, #{query_string := Qs}) ->
    Page = maps:get(<<"page">>, Qs, 1),
    Limit = maps:get(<<"limit">>, Qs, emqx_mgmt:max_row_limit()),
    {ok, Msgs} = emqx_retainer_mnesia:page_read(undefined, undefined, Page, Limit),
    {200, #{
        data => [format_message(Msg) || Msg <- Msgs],
        meta => #{page => Page, limit => Limit, count => emqx_retainer_mnesia:size(?TAB_MESSAGE)}
    }}.

with_topic(get, #{bindings := Bindings}) ->
    Topic = maps:get(topic, Bindings),
    {ok, Msgs} = emqx_retainer_mnesia:page_read(undefined, Topic, 1, 1),
    case Msgs of
        [H | _] ->
            {200, format_detail_message(H)};
        _ ->
            {404, #{
                code => <<"NOT_FOUND">>,
                message => <<"Viewed message doesn't exist">>
            }}
    end;
with_topic(delete, #{bindings := Bindings}) ->
    Topic = maps:get(topic, Bindings),
    emqx_retainer_mnesia:delete_message(undefined, Topic),
    {204}.

format_message(#message{
    id = ID,
    qos = Qos,
    topic = Topic,
    from = From,
    timestamp = Timestamp,
    headers = Headers
}) ->
    #{
        msgid => emqx_guid:to_hexstr(ID),
        qos => Qos,
        topic => Topic,
        publish_at => list_to_binary(
            calendar:system_time_to_rfc3339(
                Timestamp, [{unit, millisecond}]
            )
        ),
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
to_bin_string(Data) ->
    list_to_binary(io_lib:format("~p", [Data])).

check_backend(Type, Params, Cont) ->
    case emqx:get_config([retainer, backend, type]) of
        built_in_database ->
            Cont(Type, Params);
        _ ->
            {400, 'BAD_REQUEST', <<"This API only support built in database">>}
    end.

check_bucket_exists(
    #{
        <<"flow_control">> :=
            #{<<"batch_deliver_limiter">> := Name} = Flow
    } = Conf,
    Cont
) ->
    case erlang:binary_to_atom(Name) of
        '' ->
            %% workaround, empty string means set the value to undefined,
            %% but now, we can't store `undefined` in the config file correct,
            %% but, we can delete this field
            Cont(Conf#{
                <<"flow_control">> := maps:remove(<<"batch_deliver_limiter">>, Flow)
            });
        Bucket ->
            Path = emqx_limiter_schema:get_bucket_cfg_path(batch, Bucket),
            case emqx:get_config(Path, undefined) of
                undefined ->
                    {400, 'BAD_REQUEST', <<"The limiter bucket not exists">>};
                _ ->
                    Cont(Conf)
            end
    end;
check_bucket_exists(Conf, Cont) ->
    Cont(Conf).
