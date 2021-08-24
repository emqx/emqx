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

-export([ new/0, idle/3, maybe_reset/3, set_cache/2
        , maybe_resend/3, wait_ack/3, until_stop/3]).

-export_type([transport/0]).

-import(emqx_coap_message, [reset/1]).

-spec new() -> transport().
new() ->
    #transport{cache = undefined,
               retry_interval = 0,
               retry_count = 0}.

idle(in,
     #coap_message{type = non, method = Method} = Msg,
     _) ->
    Ret = #{next => until_stop,
            timeouts => [{stop_timeout, ?NON_LIFETIME}]},
    case Method of
        undefined ->
            ?RESET(Msg);
        _ ->
            Ret?INPUT(request, Msg)
    end;

idle(in,
     #coap_message{type = con, method = Method} = Msg,
     Transport) ->
    Ret = #{next => maybe_resend,
            timeouts =>[{stop_timeout, ?EXCHANGE_LIFETIME}]},
    case Method of
        undefined ->
            ResetMsg = reset(Msg),
            Ret#{transport => Transport#transport{cache = ResetMsg},
                 out  => ResetMsg};
        _ ->
            Ret?INPUT(request, Msg)
    end;

idle(out, #coap_message{type = non} = Msg, _) ->
    #{next => maybe_reset,
      out => Msg,
      timeouts => [{stop_timeout, ?NON_LIFETIME}]};

idle(out, Msg, Transport) ->
    _ = emqx_misc:rand_seed(),
    Timeout = ?ACK_TIMEOUT + rand:uniform(?ACK_RANDOM_FACTOR),
    #{next => wait_ack,
      transport => Transport#transport{cache = Msg},
      out => Msg,
      timeouts => [ {state_timeout, Timeout, ack_timeout}
                  , {stop_timeout, ?EXCHANGE_LIFETIME}]}.

maybe_reset(in, Message, _) ->
    Ret = #{next => until_stop},
    case Message of
        #coap_message{type = reset} ->
            ?INFO("Reset Message:~p~n", [Message]),
            Ret?INPUT(reset, Message);
        _ ->
            Ret?INPUT(response, Message)
    end.

maybe_resend(in, _, #transport{cache = Cache}) ->
    case Cache of
        undefined ->
            %% handler in processing, ignore
            ?EMPTY_RESULT;
        _ ->
            #{out => Cache}
    end.

wait_ack(in, #coap_message{type = Type} = Msg, _) ->
    Ret = #{next => utnil_stop},
    case Type of
        ack ->
            Ret?INPUT(ack, Msg);
        reset ->
            Ret?INPUT(reset, Msg);
        _ ->
            Ret?INPUT(response, Msg)
    end;

wait_ack(state_timeout,
         ack_timeout,
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
            #{next_state => until_stop,
              input => {ack_failure, Msg}}
    end.

until_stop(_, _, _) ->
    ?EMPTY_RESULT.

set_cache(Cache, Transport) ->
    Transport#transport{cache = Cache}.
