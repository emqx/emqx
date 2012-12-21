-module(emqtt_util).

-include("emqtt.hrl").

-define(CLIENT_ID_MAXLEN, 23).

-compile(export_all).

subcription_queue_name(ClientId) ->
    Base = "mqtt-subscription-" ++ ClientId ++ "qos",
    {list_to_binary(Base ++ "0"), list_to_binary(Base ++ "1")}.

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator
mqtt2amqp(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "/", ".", [global]),
                 "[\+]", "*", [global])).

amqp2mqtt(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "[\*]", "+", [global]),
                 "[\.]", "/", [global])).

valid_client_id(ClientId) ->
    ClientIdLen = length(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< ?CLIENT_ID_MAXLEN.

env(Key) ->
    case application:get_env(emqtt, Key) of
        {ok, Val} -> Val;
        undefined -> undefined
    end.

