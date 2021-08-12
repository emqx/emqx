-module(emqx_coap_transport).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

-define(ACK_TIMEOUT, 2000).
-define(ACK_RANDOM_FACTOR, 1000).
-define(MAX_RETRANSMIT, 4).
-define(EXCHANGE_LIFETIME, 247000).
-define(NON_LIFETIME, 145000).

-record(transport, { cache :: undefined | emqx_coap_message()
                   , retry_interval :: non_neg_integer()
                   , retry_count :: non_neg_integer()
                   }).

-type transport() :: #transport{}.

-export([ new/0, idle/4, maybe_reset/4
        , maybe_resend/4, wait_ack/4, until_stop/4]).

-export_type([transport/0]).

-import(emqx_coap_message, [reset/1]).

-spec new() -> transport().
new() ->
    #transport{cache = undefined,
               retry_interval = 0,
               retry_count = 0}.

idle(in,
     #coap_message{type = non, method = Method} = Msg,
     Ctx,
     _) ->
    Ret = #{next => until_stop,
            timeouts => [{stop_timeout, ?NON_LIFETIME}]},
    case Method of
        undefined ->
            ?RESET(Msg);
        _ ->
            Result = call_handler(Msg, Ctx),
            maps:merge(Ret, Result)
    end;

idle(in,
     #coap_message{type = con, method = Method} = Msg,
     Ctx,
     Transport) ->
    Ret = #{next => maybe_resend,
            timeouts =>[{stop_timeout, ?EXCHANGE_LIFETIME}]},
    case Method of
        undefined ->
            ResetMsg = reset(Msg),
            Ret#{transport => Transport#transport{cache = ResetMsg},
                 out  => ResetMsg};
        _ ->
            Result = call_handler(Msg, Ctx),
            maps:merge(Ret, Result)
    end;

idle(out, #coap_message{type = non} = Msg, _, _) ->
    #{next => maybe_reset,
      out => Msg,
      timeouts => [{stop_timeout, ?NON_LIFETIME}]};

idle(out, Msg, _, Transport) ->
    _ = emqx_misc:rand_seed(),
    Timeout = ?ACK_TIMEOUT + rand:uniform(?ACK_RANDOM_FACTOR),
    #{next => wait_ack,
      transport => Transport#transport{cache = Msg},
      out => Msg,
      timeouts => [ {state_timeout, Timeout, ack_timeout}
                  , {stop_timeout, ?EXCHANGE_LIFETIME}]}.

maybe_reset(in, Message, _, _) ->
    case Message of
        #coap_message{type = reset} ->
            ?INFO("Reset Message:~p~n", [Message]);
        _ ->
            ok
    end,
    ?EMPTY_RESULT.

maybe_resend(in, _, _, #transport{cache = Cache}) ->
    #{out => Cache}.

wait_ack(in, #coap_message{type = Type}, _, _) ->
    case Type of
        ack ->
            #{next => until_stop};
        reset ->
            #{next => until_stop};
        _ ->
            ?EMPTY_RESULT
    end;

wait_ack(state_timeout,
         ack_timeout,
         _,
         #transport{cache = Msg,
                    retry_interval = Timeout,
                    retry_count = Count} =Transport) ->
    case Count < ?MAX_RETRANSMIT of
        true ->
            Timeout2 = Timeout * 2,
            #{transport => Transport#transport{retry_interval = Timeout2,
                                               retry_count = Count + 1},
              out => Msg,
              timeouts => [{state_timeout, Timeout2, ack_timeout}]};
        _ ->
            #{next_state => until_stop}
    end.

until_stop(_, _, _, _) ->
    ?EMPTY_RESULT.

call_handler(#coap_message{options = Opts} = Msg, Ctx) ->
    case maps:get(uri_path, Opts, undefined) of
        [<<"ps">> | RestPath] ->
            emqx_coap_pubsub_handler:handle_request(RestPath, Msg, Ctx);
        [<<"mqtt">> | RestPath] ->
            emqx_coap_mqtt_handler:handle_request(RestPath, Msg, Ctx);
        _ ->
            ?REPLY({error, bad_request}, Msg)
    end.
