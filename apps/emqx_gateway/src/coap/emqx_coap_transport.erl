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

-spec new() -> transport().
new() ->
    #transport{cache = undefined,
               retry_interval = 0,
               retry_count = 0}.

idle(in,
     #coap_message{type = non, id = MsgId, method = Method} = Msg,
     _,
     #{resource := Resource} = Cfg) ->
    Ret = #{next => until_stop,
            timeouts => [{stop_timeout, ?NON_LIFETIME}]},
    case Method of
        undefined ->
            Ret#{out => #coap_message{type = reset, id = MsgId}};
        _ ->
            case erlang:apply(Resource, Method, [Msg, Cfg]) of
                #coap_message{} = Result ->
                    Ret#{out => Result};
                {has_sub, Result, Sub} ->
                    Ret#{out => Result, subscribe => Sub};
                error ->
                    Ret#{out =>
                             emqx_coap_message:response({error, internal_server_error}, Msg)}
            end
    end;

idle(in,
     #coap_message{id = MsgId,
                   type = con,
                   method = Method} = Msg,
     Transport,
     #{resource := Resource} = Cfg) ->
    Ret = #{next => maybe_resend,
            timeouts =>[{stop_timeout, ?EXCHANGE_LIFETIME}]},
    case Method of
        undefined ->
            ResetMsg = #coap_message{type = reset, id = MsgId},
            Ret#{transport => Transport#transport{cache = ResetMsg},
                 out  => ResetMsg};
        _ ->
            {RetMsg, SubInfo} =
                case erlang:apply(Resource, Method, [Msg, Cfg]) of
                    #coap_message{} = Result ->
                        {Result, undefined};
                    {has_sub, Result, Sub} ->
                        {Result, Sub};
                    error ->
                        {emqx_coap_message:response({error, internal_server_error}, Msg),
                         undefined}
                end,
            RetMsg2 = RetMsg#coap_message{type = ack},
            Ret#{out => RetMsg2,
                 transport => Transport#transport{cache = RetMsg2},
                 subscribe => SubInfo}
    end;

idle(out, #coap_message{type = non} = Msg, _, _) ->
    #{next => maybe_reset,
      out => Msg,
      timeouts => [{stop_timeout, ?NON_LIFETIME}]};

idle(out, Msg, Transport, _) ->
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
