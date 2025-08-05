%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_dispatch_bif).

-moduledoc """
This module implements bifs for the variform expression evaluation
used in the MQ consumer for dispatch strategies.
""".

-export([
    topic/1,
    clientid/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% TODO
%% Add more bifs

topic(Message) ->
    emqx_message:topic(Message).

clientid(Message) ->
    emqx_message:from(Message).
