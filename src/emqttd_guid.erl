%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%%
%%% Generate global unique id for mqtt message.
%%%
%%% --------------------------------------------------------
%%% |        Timestamp       |  NodeID + PID  |  Sequence  | 
%%% |<------- 64bits ------->|<--- 48bits --->|<- 16bits ->|
%%% --------------------------------------------------------
%%% 
%%% 1. Timestamp: erlang:system_time if Erlang >= R18, otherwise os:timestamp
%%% 2. NodeId:    encode node() to 2 bytes integer
%%% 3. Pid:       encode pid to 4 bytes integer
%%% 4. Sequence:  2 bytes sequence in one process
%%%
%%% @end
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_guid).

-export([gen/0, new/0, timestamp/1]).

-define(MAX_SEQ, 16#FFFF).

-type guid() :: <<_:128>>.

%%------------------------------------------------------------------------------
%% @doc Generate a global unique id.
%% @end
%%------------------------------------------------------------------------------
-spec gen() -> guid().
gen() ->
    Guid = case get(guid) of
        undefined        -> new();
        {_Ts, NPid, Seq} -> next(NPid, Seq)
    end,
    put(guid, Guid), bin(Guid).

new() ->
    {ts(), npid(), 0}.

-spec timestamp(guid()) -> integer().
timestamp(<<Ts:64, _/binary>>) ->
    Ts.

next(NPid, Seq) when Seq >= ?MAX_SEQ ->
    {ts(), NPid, 0};
next(NPid, Seq) ->
    {ts(), NPid, Seq + 1}.

bin({Ts, NPid, Seq}) ->
    <<Ts:64, NPid:48, Seq:16>>.

ts() ->
    case erlang:function_exported(erlang, system_time, 1) of
        true -> %% R18
            erlang:system_time(micro_seconds);
        false ->
            {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
            (MegaSeconds * 1000000 + Seconds) * 1000000 + MicroSeconds
    end.

%% Copied from https://github.com/okeuday/uuid.git.
npid() ->
    <<NodeD01, NodeD02, NodeD03, NodeD04, NodeD05,
      NodeD06, NodeD07, NodeD08, NodeD09, NodeD10,
      NodeD11, NodeD12, NodeD13, NodeD14, NodeD15,
      NodeD16, NodeD17, NodeD18, NodeD19, NodeD20>> =
      crypto:hash(sha, erlang:list_to_binary(erlang:atom_to_list(node()))),

    % later, when the pid format changes, handle the different format
    ExternalTermFormatVersion = 131,
    PidExtType = 103,
    <<ExternalTermFormatVersion:8,
      PidExtType:8,
      PidBin/binary>> = erlang:term_to_binary(self()),
    % 72 bits for the Erlang pid
    <<PidID1:8, PidID2:8, PidID3:8, PidID4:8, % ID (Node specific, 15 bits)
      PidSR1:8, PidSR2:8, PidSR3:8, PidSR4:8, % Serial (extra uniqueness)
      PidCR1:8                       % Node Creation Count
      >> = binary:part(PidBin, erlang:byte_size(PidBin), -9),

    % reduce the 160 bit NodeData checksum to 16 bits
    NodeByte1 = ((((((((NodeD01 bxor NodeD02)
                       bxor NodeD03)
                      bxor NodeD04)
                     bxor NodeD05)
                    bxor NodeD06)
                   bxor NodeD07)
                  bxor NodeD08)
                 bxor NodeD09)
                bxor NodeD10,
    NodeByte2 = (((((((((NodeD11 bxor NodeD12)
                        bxor NodeD13)
                       bxor NodeD14)
                      bxor NodeD15)
                     bxor NodeD16)
                    bxor NodeD17)
                   bxor NodeD18)
                  bxor NodeD19)
                 bxor NodeD20)
                bxor PidCR1,

    % reduce the Erlang pid to 32 bits
    PidByte1 = PidID1 bxor PidSR4,
    PidByte2 = PidID2 bxor PidSR3,
    PidByte3 = PidID3 bxor PidSR2,
    PidByte4 = PidID4 bxor PidSR1,

    <<NPid:48>> = <<NodeByte1:8, NodeByte2:8,
                    PidByte1:8, PidByte2:8,
                    PidByte3:8, PidByte4:8>>,
    NPid.

