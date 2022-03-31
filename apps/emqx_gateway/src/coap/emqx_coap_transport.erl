-module(emqx_coap_transport).

-include_lib("emqx/include/logger.hrl").
-include("src/coap/include/emqx_coap.hrl").

-define(ACK_TIMEOUT, 2000).
-define(ACK_RANDOM_FACTOR, 1000).
-define(MAX_RETRANSMIT, 4).
-define(EXCHANGE_LIFETIME, 247000).
-define(NON_LIFETIME, 145000).

-type request_context() :: any().

-record(transport, {
    cache :: undefined | coap_message(),
    req_context :: request_context(),
    retry_interval :: non_neg_integer(),
    retry_count :: non_neg_integer(),
    observe :: non_neg_integer() | undefined
}).

-type transport() :: #transport{}.

-export([
    new/0, new/1,
    idle/3,
    maybe_reset/3,
    set_cache/2,
    maybe_resend_4request/3,
    wait_ack/3,
    until_stop/3,
    observe/3,
    maybe_resend_4response/3
]).

-export_type([transport/0]).

-import(emqx_coap_medium, [
    empty/0,
    reset/2,
    proto_out/2,
    out/1, out/2,
    proto_out/1,
    reply/2
]).

-spec new() -> transport().
new() ->
    new(undefined).

new(ReqCtx) ->
    #transport{
        cache = undefined,
        retry_interval = 0,
        retry_count = 0,
        req_context = ReqCtx
    }.

idle(
    in,
    #coap_message{type = non, method = Method} = Msg,
    _
) ->
    case Method of
        undefined ->
            reset(Msg, #{next => stop});
        _ ->
            proto_out(
                {request, Msg},
                #{
                    next => until_stop,
                    timeouts =>
                        [{stop_timeout, ?NON_LIFETIME}]
                }
            )
    end;
idle(
    in,
    #coap_message{type = con, method = Method} = Msg,
    _
) ->
    case Method of
        undefined ->
            reset(Msg, #{next => stop});
        _ ->
            proto_out(
                {request, Msg},
                #{
                    next => maybe_resend_4request,
                    timeouts => [{stop_timeout, ?EXCHANGE_LIFETIME}]
                }
            )
    end;
idle(out, #coap_message{type = non} = Msg, _) ->
    out(Msg, #{
        next => maybe_reset,
        timeouts => [{stop_timeout, ?NON_LIFETIME}]
    });
idle(out, Msg, Transport) ->
    _ = emqx_misc:rand_seed(),
    Timeout = ?ACK_TIMEOUT + rand:uniform(?ACK_RANDOM_FACTOR),
    out(Msg, #{
        next => wait_ack,
        transport => Transport#transport{cache = Msg},
        timeouts => [
            {state_timeout, Timeout, ack_timeout},
            {stop_timeout, ?EXCHANGE_LIFETIME}
        ]
    }).

maybe_resend_4request(in, Msg, Transport) ->
    maybe_resend(Msg, true, Transport).

maybe_resend_4response(in, Msg, Transport) ->
    maybe_resend(Msg, false, Transport).

maybe_resend(Msg, IsExpecteReq, #transport{cache = Cache}) ->
    IsExpected = emqx_coap_message:is_request(Msg) =:= IsExpecteReq,
    case IsExpected of
        true ->
            case Cache of
                undefined ->
                    %% handler in processing, ignore
                    empty();
                _ ->
                    out(Cache)
            end;
        _ ->
            reset(Msg, #{next => stop})
    end.

maybe_reset(
    in,
    #coap_message{type = Type, method = Method} = Message,
    #transport{req_context = Ctx} = Transport
) ->
    Ret = #{next => stop},
    CtxMsg = {Ctx, Message},
    if
        Type =:= reset ->
            proto_out({reset, CtxMsg}, Ret);
        is_tuple(Method) ->
            on_response(
                Message,
                Transport,
                if
                    Type =:= non -> until_stop;
                    true -> maybe_resend_4response
                end
            );
        true ->
            reset(Message, Ret)
    end.

wait_ack(in, #coap_message{type = Type, method = Method} = Msg, #transport{req_context = Ctx}) ->
    CtxMsg = {Ctx, Msg},
    case Type of
        reset ->
            proto_out({reset, CtxMsg}, #{next => stop});
        _ ->
            case Method of
                undefined ->
                    %% empty ack, keep transport to recv response
                    proto_out({ack, CtxMsg});
                {_, _} ->
                    %% ack with payload
                    proto_out({response, CtxMsg}, #{next => stop});
                _ ->
                    reset(Msg, #{next => stop})
            end
    end;
wait_ack(
    state_timeout,
    ack_timeout,
    #transport{
        cache = Msg,
        retry_interval = Timeout,
        retry_count = Count
    } = Transport
) ->
    case Count < ?MAX_RETRANSMIT of
        true ->
            Timeout2 = Timeout * 2,
            out(
                Msg,
                #{
                    transport => Transport#transport{
                        retry_interval = Timeout2,
                        retry_count = Count + 1
                    },
                    timeouts => [{state_timeout, Timeout2, ack_timeout}]
                }
            );
        _ ->
            proto_out({ack_failure, Msg}, #{next_state => stop})
    end.

observe(
    in,
    #coap_message{method = Method} = Message,
    #transport{observe = Observe} = Transport
) ->
    case Method of
        {ok, _} ->
            case emqx_coap_message:get_option(observe, Message, Observe) of
                Observe ->
                    %% repeatd notify, ignore
                    empty();
                NewObserve ->
                    on_response(
                        Message,
                        Transport#transport{observe = NewObserve},
                        ?FUNCTION_NAME
                    )
            end;
        {error, _} ->
            #{next => stop};
        _ ->
            emqx_coap_message:reset(Message)
    end.

until_stop(_, _, _) ->
    empty().

set_cache(Cache, Transport) ->
    Transport#transport{cache = Cache}.

on_response(
    #coap_message{type = Type} = Message,
    #transport{req_context = Ctx} = Transport,
    NextState
) ->
    CtxMsg = {Ctx, Message},
    if
        Type =:= non ->
            proto_out({response, CtxMsg}, #{next => NextState});
        Type =:= con ->
            Ack = emqx_coap_message:ack(Message),
            proto_out(
                {response, CtxMsg},
                out(Ack, #{
                    next => NextState,
                    transport => Transport#transport{cache = Ack}
                })
            );
        true ->
            emqx_coap_message:reset(Message)
    end.
