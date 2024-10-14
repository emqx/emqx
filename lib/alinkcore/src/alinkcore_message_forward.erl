%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 03. 4月 2023 下午5:53
%%%-------------------------------------------------------------------
-module(alinkcore_message_forward).

%% API
-export([
    forward/2,
    dispatch/2
]).

-include("alinkcore.hrl").

%%%===================================================================
%%% API
%%%===================================================================
forward(#session{node = Node} = Session, Message) when Node =/= node() ->
    case emqx_rpc:cast(Message#message.topic, Node, ?MODULE, forward, [Session, Message]) of
        true ->
            ok;
        {badrpc, Reason} ->
            logger:error("Ansync forward msg to ~s failed due to ~p", [Node, Reason])
    end;
forward(Session, Message)->
    dispatch(Session, Message).



dispatch(#session{pid = Pid}, Message) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {deliver, Message#message.topic, Message};
        false ->
            logger:error("dispatch message ~p failed, process ~p is not alive",
                [Message, Pid])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================