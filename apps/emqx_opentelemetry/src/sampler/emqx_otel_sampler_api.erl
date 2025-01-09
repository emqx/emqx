%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_sampler_api).

-behaviour(minirest_api).

-include("emqx_otel_trace.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, enum/1, ref/1, array/1]).

-export([
    namespace/0,
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

%% operation functions
-export([
    whitelist/2
]).

-define(API_TAGS, [<<"Opentelemetry">>]).
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(INTERNAL_ERROR, 'INTERNAL_ERROR').

-define(RESP_INTERNAL_ERROR(MSG), {500, #{code => ?INTERNAL_ERROR, message => MSG}}).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/opentelemetry/whitelist/:type"
    ].

schema("/opentelemetry/whitelist/:type") ->
    #{
        'operationId' => whitelist,
        get => #{
            tags => ?API_TAGS,
            description => ?DESC(sample_white_list_get),
            parameters => [ref(white_list_type)],
            responses =>
                #{
                    200 => swagger_with_example()
                }
        },
        post => #{
            tags => ?API_TAGS,
            description => ?DESC(sample_white_list_post),
            parameters => [ref(white_list_type)],
            'requestBody' => swagger_with_example(),
            responses =>
                #{
                    204 => <<"Created">>,
                    500 => emqx_dashboard_swagger:error_codes(
                        [?INTERNAL_ERROR], <<"Internal Service Error">>
                    )
                }
        },
        delete =>
            #{
                tags => ?API_TAGS,
                description => ?DESC(sample_white_list_delete),
                parameters => [ref(white_list_type)],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        500 => emqx_dashboard_swagger:error_codes(
                            [?INTERNAL_ERROR], <<"Internal Service Error">>
                        )
                    }
            }
    }.

fields(clientid) ->
    [
        {clientid,
            mk(
                binary(),
                #{
                    desc => ?DESC(clientid),
                    example => <<"client1">>
                }
            )}
    ];
fields(topic) ->
    [
        {topic,
            mk(
                binary(),
                #{
                    desc => ?DESC(topic),
                    example => <<"topic/#">>
                }
            )}
    ];
fields(white_list_type) ->
    [
        {type,
            mk(
                enum([clientid, topic]),
                #{
                    in => path,
                    required => true,
                    desc => ?DESC(white_list_type),
                    example => <<"clientid">>
                }
            )}
    ].
%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

whitelist(get, #{bindings := #{type := Type}}) ->
    Res = [
        V
     || #?EMQX_OTEL_SAMPLER{type = {_, V}} <- emqx_otel_sampler:get_rules(Type)
    ],
    {200, Res};
whitelist(post, #{bindings := #{type := Type}, body := Body}) when is_list(Body) ->
    case ensure_rules_is_valid(Type, Body) of
        ok ->
            %% Purge all old data and store new
            ok = emqx_otel_sampler:purge_rules(Type),
            ok = lists:foreach(fun(Val) -> store_rules(Type, Val) end, Body),
            {204};
        {error, too_many_rules} ->
            {400, #{
                code => ?BAD_REQUEST,
                message =>
                    binfmt(
                        <<"The number of '~ts' exceeds the white list maximum limit.">>, [Type]
                    )
            }}
    end;
whitelist(delete, #{bindings := #{type := Type}}) ->
    try emqx_otel_sampler:purge_rules(Type) of
        ok -> {204}
    catch
        _:Error ->
            ?RESP_INTERNAL_ERROR(emqx_utils:readable_error_msg(Error))
    end.

%%--------------------------------------------------------------------
%% Internal Helpers
%%--------------------------------------------------------------------

swagger_with_example() ->
    emqx_dashboard_swagger:schema_with_examples(
        array(binary()),
        #{
            clientid_white_list => #{
                summary => <<"ClientId White list">>,
                value => [<<"clientid">>, <<"clientid2">>]
            },

            topic_white_list => #{
                summary => <<"Topic White list">>,
                value => [<<"topic/#">>, <<"topic/2">>]
            }
        }
    ).

ensure_rules_is_valid(Type, Rules) ->
    case length(Rules) =< rules_max(Type) of
        true -> ok;
        false -> {error, too_many_rules}
    end.

store_rules(clientid, ClientID) ->
    emqx_otel_sampler:store_rule(clientid, ClientID);
store_rules(topic, Topic) ->
    emqx_otel_sampler:store_rule(topic, Topic).

rules_max(clientid) -> get_rule_max(clientid_match_rules_max);
rules_max(topic) -> get_rule_max(topic_match_rules_max).

-define(RULE_MAX_CONFIG_KEY_PREFIX, [opentelemetry, traces, filter, e2e_tracing_options]).
get_rule_max(Key) ->
    emqx:get_config(?RULE_MAX_CONFIG_KEY_PREFIX ++ [Key]).

binfmt(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).
