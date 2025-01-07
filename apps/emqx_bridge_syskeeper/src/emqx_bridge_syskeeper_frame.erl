%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQ X Bridge Sysk Frame
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_frame).

%% API
-export([
    versions/0,
    current_version/0,
    make_state_with_conf/1,
    make_state/1,
    encode/3,
    parse/2,
    parse_handshake/1
]).

-export([
    bool2int/1,
    int2bool/1,
    marshaller/1,
    serialize_variable_byte_integer/1,
    parse_variable_byte_integer/1
]).

-export_type([state/0, versions/0, handshake/0, forward/0, packet/0]).

-include("emqx_bridge_syskeeper.hrl").

-type state() :: #{
    handler := atom(),
    version := versions(),
    ack => boolean()
}.

-type versions() :: 1.

-type handshake() :: #{type := handshake, version := versions()}.
-type forward() :: #{type := forward, ack := boolean(), messages := list(map())}.
-type heartbeat() :: #{type := heartbeat}.

-type packet() ::
    handshake()
    | forward()
    | heartbeat().

-callback version() -> versions().
-callback encode(packet_type_val(), packet_data(), state()) -> binary().
-callback parse(packet_type(), binary(), state()) -> packet().

-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).
-define(MULTIPLIER_MAX, 16#200000).

-export_type([packet_type/0]).

%%-------------------------------------------------------------------
%%% API
%%-------------------------------------------------------------------
-spec versions() -> list(versions()).
versions() ->
    [1].

-spec current_version() -> versions().
current_version() ->
    1.

-spec make_state_with_conf(map()) -> state().
make_state_with_conf(#{ack_mode := Mode}) ->
    State = make_state(current_version()),
    State#{ack => Mode =:= need_ack}.

-spec make_state(versions()) -> state().
make_state(Version) ->
    case lists:member(Version, versions()) of
        true ->
            Handler = erlang:list_to_existing_atom(
                io_lib:format("emqx_bridge_syskeeper_frame_v~B", [Version])
            ),
            #{
                handler => Handler,
                version => Version
            };
        _ ->
            erlang:throw({unsupport_version, Version})
    end.

-spec encode(packet_type(), term(), state()) -> binary().
encode(Type, Data, #{handler := Handler} = State) ->
    Handler:encode(packet_type_val(Type), Data, State).

-spec parse(binary(), state()) -> _.
parse(<<TypeVal:4, _:4, _/binary>> = Bin, #{handler := Handler} = State) ->
    Type = to_packet_type(TypeVal),
    Handler:parse(Type, Bin, State).

parse_handshake(Data) ->
    State = make_state(1),
    parse_handshake(Data, State).

parse_handshake(Data, #{version := Version} = State) ->
    case parse(Data, State) of
        {ok, #{type := handshake, version := Version} = Shake} ->
            {ok, {State, Shake}};
        {ok, #{type := handshake, version := NewVersion}} ->
            State2 = make_state(NewVersion),
            parse_handshake(Data, State2);
        Error ->
            Error
    end.

bool2int(true) ->
    1;
bool2int(_) ->
    0.

int2bool(1) ->
    true;
int2bool(_) ->
    false.

marshaller(Item) when is_binary(Item) ->
    erlang:binary_to_term(Item);
marshaller(Item) ->
    erlang:term_to_binary(Item).

serialize_variable_byte_integer(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialize_variable_byte_integer(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialize_variable_byte_integer(N div ?HIGHBIT))/binary>>.

parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).

%%-------------------------------------------------------------------
%%% Internal functions
%%-------------------------------------------------------------------
to_packet_type(?TYPE_HANDSHAKE) ->
    handshake;
to_packet_type(?TYPE_FORWARD) ->
    forward;
to_packet_type(?TYPE_HEARTBEAT) ->
    heartbeat.

packet_type_val(handshake) ->
    ?TYPE_HANDSHAKE;
packet_type_val(forward) ->
    ?TYPE_FORWARD;
packet_type_val(heartbeat) ->
    ?TYPE_HEARTBEAT.

parse_variable_byte_integer(<<1:1, _Len:7, _Rest/binary>>, Multiplier, _Value) when
    Multiplier > ?MULTIPLIER_MAX
->
    {error, malformed_variable_byte_integer};
parse_variable_byte_integer(<<1:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    parse_variable_byte_integer(Rest, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_variable_byte_integer(<<0:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    {ok, Value + Len * Multiplier, Rest};
parse_variable_byte_integer(<<>>, _Multiplier, _Value) ->
    {error, incomplete}.
