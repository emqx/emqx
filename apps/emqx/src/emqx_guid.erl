%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Generate global unique id for mqtt message.
%%
%% --------------------------------------------------------
%% |        Timestamp       |  NodeID + PID  |  Sequence  |
%% |<------- 64bits ------->|<--- 48bits --->|<- 16bits ->|
%% --------------------------------------------------------
%%
%% 1. Timestamp: erlang:system_time if Erlang >= R18, otherwise os:timestamp
%% 2. NodeId:    encode node() to 2 bytes integer
%% 3. Pid:       encode pid to 4 bytes integer
%% 4. Sequence:  2 bytes sequence in one process
%%
%% @end

-module(emqx_guid).

-export([
    gen/0,
    new/0,
    timestamp/1,
    to_hexstr/1,
    from_hexstr/1
]).

-export_type([guid/0]).

-define(TAG_VERSION, 131).
-define(PID_EXT, 103).
-define(NEW_PID_EXT, 88).

-define(MAX_SEQ, 16#FFFF).

-type guid() :: <<_:128>>.

%% @doc Generate a global unique id.
-spec gen() -> guid().
gen() ->
    Guid =
        case get(guid) of
            undefined -> new();
            {_Ts, NPid, Seq} -> next(NPid, Seq)
        end,
    put(guid, Guid),
    bin(Guid).

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

ts() -> erlang:system_time(micro_seconds).

%% Copied from https://github.com/okeuday/uuid.git.
npid() ->
    <<NodeD01, NodeD02, NodeD03, NodeD04, NodeD05, NodeD06, NodeD07, NodeD08, NodeD09, NodeD10,
        NodeD11, NodeD12, NodeD13, NodeD14, NodeD15, NodeD16, NodeD17, NodeD18, NodeD19,
        NodeD20>> =
        crypto:hash(sha, erlang:atom_to_binary(node())),

    PidBin =
        case erlang:term_to_binary(self()) of
            <<?TAG_VERSION, ?PID_EXT, B/binary>> ->
                binary:part(B, erlang:byte_size(B), -9);
            % format supported in Erlang/OTP 19.0-rc1
            % required for Erlang/OTP 23.0 (and Erlang/OTP 22.0-rc2)
            <<?TAG_VERSION, ?NEW_PID_EXT, B/binary>> ->
                binary:part(B, erlang:byte_size(B), -12)
        end,

    % 72/86 bits for the Erlang pid

    % ID (Node specific, 15 bits)
    <<PidID1:8, PidID2:8, PidID3:8, PidID4:8,
        % Serial (extra uniqueness)
        PidSR1:8, PidSR2:8, PidSR3:8, PidSR4:8,
        % Node Creation Count
        PidCreation/binary>> = PidBin,

    PidCR1 =
        case PidCreation of
            <<D1>> ->
                D1;
            <<D1, D2, D3, D4>> ->
                D1 bxor D2 bxor D3 bxor D4
        end,

    % reduce the 160 bit NodeData checksum to 16 bits
    NodeByte1 =
        ((((((((NodeD01 bxor NodeD02) bxor
            NodeD03) bxor
            NodeD04) bxor
            NodeD05) bxor
            NodeD06) bxor
            NodeD07) bxor
            NodeD08) bxor
            NodeD09) bxor
            NodeD10,
    NodeByte2 =
        (((((((((NodeD11 bxor NodeD12) bxor
            NodeD13) bxor
            NodeD14) bxor
            NodeD15) bxor
            NodeD16) bxor
            NodeD17) bxor
            NodeD18) bxor
            NodeD19) bxor
            NodeD20) bxor
            PidCR1,

    % reduce the Erlang pid to 32 bits
    PidByte1 = PidID1 bxor PidSR4,
    PidByte2 = PidID2 bxor PidSR3,
    PidByte3 = PidID3 bxor PidSR2,
    PidByte4 = PidID4 bxor PidSR1,

    <<NPid:48>> = <<NodeByte1:8, NodeByte2:8, PidByte1:8, PidByte2:8, PidByte3:8, PidByte4:8>>,
    NPid.

to_hexstr(I) when byte_size(I) =:= 16 ->
    emqx_utils:bin_to_hexstr(I, upper).

from_hexstr(S) when byte_size(S) =:= 32 ->
    emqx_utils:hexstr_to_bin(S).
