%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQ X Bridge Sysk Frame version 1
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_frame_v1).

%% API
-export([
    version/0,
    encode/3,
    parse/3
]).

-behaviour(emqx_bridge_syskeeper_frame).

-include("emqx_bridge_syskeeper.hrl").

-define(B2I(X), emqx_bridge_syskeeper_frame:bool2int((X))).
-define(I2B(X), emqx_bridge_syskeeper_frame:int2bool((X))).

-import(emqx_bridge_syskeeper_frame, [
    serialize_variable_byte_integer/1, parse_variable_byte_integer/1, marshaller/1
]).

%%-------------------------------------------------------------------
%%% API
%%-------------------------------------------------------------------
version() ->
    1.

encode(?TYPE_HANDSHAKE = Type, _, _) ->
    Version = version(),
    <<Type:4, 0:4, Version:8>>;
encode(?TYPE_FORWARD = Type, Messages, #{ack := Ack}) ->
    encode_forward(Messages, Type, Ack);
encode(?TYPE_HEARTBEAT = Type, _, _) ->
    <<Type:4, 0:4>>.

-dialyzer({nowarn_function, parse/3}).
parse(handshake, <<_:4, _:4, Version:8>>, _) ->
    {ok, #{type => handshake, version => Version}};
parse(forward, Bin, _) ->
    parse_forward(Bin);
parse(heartbeat, <<_:4, _:4>>, _) ->
    {ok, #{type => heartbeat}}.

%%-------------------------------------------------------------------
%%% Internal functions
%%-------------------------------------------------------------------
encode_forward(Messages, Type, Ack) ->
    AckVal = ?B2I(Ack),
    Data = marshaller(Messages),
    Len = erlang:byte_size(Data),
    LenVal = serialize_variable_byte_integer(Len),
    <<Type:4, AckVal:4, LenVal/binary, Data/binary>>.

parse_forward(<<_:4, AckVal:4, Bin/binary>>) ->
    case parse_variable_byte_integer(Bin) of
        {ok, Len, Rest} ->
            <<MsgBin:Len/binary, _/binary>> = Rest,
            {ok, #{
                type => forward,
                ack => ?I2B(AckVal),
                messages => marshaller(MsgBin)
            }};
        Error ->
            Error
    end.
