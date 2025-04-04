%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The frame parser for ExProto
-module(emqx_exproto_frame).

-behaviour(emqx_gateway_frame).

-export([
    initial_parse_state/1,
    serialize_opts/0,
    parse/2,
    serialize_pkt/2,
    format/1,
    is_message/1,
    type/1
]).

initial_parse_state(_) ->
    #{}.

serialize_opts() ->
    #{}.

parse(Data, State) ->
    {ok, Data, <<>>, State}.

serialize_pkt(Data, _Opts) ->
    Data.

format(Data) ->
    io_lib:format("~p", [Data]).

is_message(_) -> true.

type(_) -> unknown.
