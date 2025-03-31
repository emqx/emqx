%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_internal).

-export([init/1]).

-export([handle/3]).

-spec init(Config :: map()) -> HandlerOptions :: term().
init(#{topics := Topics}) ->
    emqx_auto_subscribe_placeholder:generate(Topics).

-spec handle(ClientInfo :: map(), ConnInfo :: map(), HandlerOptions :: term()) ->
    TopicTables :: list().
handle(ClientInfo, ConnInfo, PlaceHolders) ->
    emqx_auto_subscribe_placeholder:to_topic_table(PlaceHolders, ClientInfo, ConnInfo).
