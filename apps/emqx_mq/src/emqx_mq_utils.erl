%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_utils).

-include_lib("emqx_mq_internal.hrl").

-define(DISPATCH_BIF_MOD_STR, "emqx_mq_consumer_dispatch_bif.").
-define(DISPATCH_BIF_MOD_SHORTCUT, "m.").

-export([
    mq_info/2,
    dispatch_variform_compile/1,
    transform_dispatch_variform_expr/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

mq_info(ChannelPid, TopicFilter) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_mq_info{receiver = Self, topic_filter = TopicFilter}),
    receive
        {Self, InfoList} ->
            InfoList
    end.

dispatch_variform_compile(Expression) ->
    {ok, Compiled} = emqx_variform:compile(Expression),
    transform_dispatch_variform_expr(Compiled).

transform_dispatch_variform_expr(#{form := Form} = Compiled) ->
    Compiled#{form := traverse_transform_bifs(Form)}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

traverse_transform_bifs({call, FnName, Args}) ->
    FQFnName = fully_qualify_local_bif(FnName),
    {call, FQFnName, lists:map(fun traverse_transform_bifs/1, Args)};
traverse_transform_bifs({array, Elems}) ->
    {array, lists:map(fun traverse_transform_bifs/1, Elems)};
traverse_transform_bifs(Node) ->
    Node.

fully_qualify_local_bif(?DISPATCH_BIF_MOD_SHORTCUT ++ FnName) ->
    ?DISPATCH_BIF_MOD_STR ++ FnName;
fully_qualify_local_bif(FnName) ->
    FnName.
