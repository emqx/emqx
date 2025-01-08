%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(pulsar_echo_consumer).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% pulsar consumer API
-export([init/2, handle_message/3]).

init(Topic, Args) ->
    ct:pal("consumer init: ~p", [#{topic => Topic, args => Args}]),
    SendTo = maps:get(send_to, Args),
    ?tp(pulsar_echo_consumer_init, #{topic => Topic}),
    {ok, #{topic => Topic, send_to => SendTo}}.

handle_message(Message, Payloads, State) ->
    #{send_to := SendTo, topic := Topic} = State,
    ct:pal(
        "pulsar consumer received:\n  ~p",
        [#{message => Message, payloads => Payloads}]
    ),
    SendTo ! {pulsar_message, #{topic => Topic, message => Message, payloads => Payloads}},
    ?tp(pulsar_echo_consumer_message, #{topic => Topic, message => Message, payloads => Payloads}),
    {ok, 'Individual', State}.
