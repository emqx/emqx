%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rewrite_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_modules.hrl").

-export([api_spec/0, paths/0, schema/1]).

-export([topic_rewrite/2]).

-define(MAX_RULES_LIMIT, 20).

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').
-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    ["/mqtt/topic_rewrite"].

schema("/mqtt/topic_rewrite") ->
    #{
        'operationId' => topic_rewrite,
        get => #{
            tags => ?API_TAG_MQTT,
            description => ?DESC(list_topic_rewrite_api),
            responses => #{
                200 => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(emqx_modules_schema, "rewrite")),
                    #{desc => ?DESC(list_topic_rewrite_api)}
                )
            }
        },
        put => #{
            description => ?DESC(update_topic_rewrite_api),
            tags => ?API_TAG_MQTT,
            'requestBody' => hoconsc:mk(
                hoconsc:array(
                    hoconsc:ref(emqx_modules_schema, "rewrite")
                ),
                #{}
            ),
            responses => #{
                200 => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(emqx_modules_schema, "rewrite")),
                    #{desc => ?DESC(update_topic_rewrite_api)}
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST],
                    ?DESC(update_topic_rewrite_api_response400)
                ),
                413 => emqx_dashboard_swagger:error_codes(
                    [?EXCEED_LIMIT],
                    ?DESC(update_topic_rewrite_api_response413)
                )
            }
        }
    }.

topic_rewrite(get, _Params) ->
    {200, emqx_rewrite:list()};
topic_rewrite(put, #{body := Body}) ->
    case length(Body) < ?MAX_RULES_LIMIT of
        true ->
            try
                ok = emqx_rewrite:update(Body),
                {200, emqx_rewrite:list()}
            catch
                throw:#{
                    kind := validation_error,
                    reason := #{
                        msg := "cannot_use_wildcard_for_destination_topic",
                        invalid_topics := InvalidTopics
                    }
                } ->
                    Message = get_invalid_wildcard_topic_msg(InvalidTopics),
                    {400, #{code => ?BAD_REQUEST, message => Message}}
            end;
        _ ->
            Message = iolist_to_binary(
                io_lib:format("Max rewrite rules count is ~p", [?MAX_RULES_LIMIT])
            ),
            {413, #{code => ?EXCEED_LIMIT, message => Message}}
    end.

get_invalid_wildcard_topic_msg(InvalidTopics) ->
    iolist_to_binary(
        io_lib:format("Cannot use wildcard for destination topic. Invalid topics: ~p", [
            InvalidTopics
        ])
    ).
