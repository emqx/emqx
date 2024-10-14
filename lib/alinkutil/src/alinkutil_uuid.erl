%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%       uuid util
%%%       copied from https://github.com/okeuday/uuid
%%% ==Erlang UUID Generation==
%%% [http://www.ietf.org/rfc/rfc4122.txt] is the reference for official UUIDs.
%%% This implementation provides a version 1 UUID that includes both the
%%% Erlang pid identifier (ID, Serial, Creation) and the distributed Erlang
%%% node name within the 48 bit node ID.  To make room for the Erlang pid
%%% identifier, the 48 bits from the MAC address
%%% (i.e., 3 OCI (Organizationally Unique Identifier) bytes and
%%% 3 NIC (Network Interface Controller) specific bytes) and
%%% the distributed Erlang node name are bitwise-XORed down to 16 bits.
%%% The Erlang pid is bitwise-XORed from 72 bits down to 32 bits.
%%% The version 3 (MD5), version 4 (random), and version 5 (SHA)
%%% methods are provided as specified within the RFC.
%%%
%%% The ordered version 1 variant is not present in the RFC,
%%% though it is the most standards-compliant way of providing
%%% timestamp UUID ordering.  Timestamp ordering has been used in many
%%% non-standard UUID formats based on version 1 and typically limited to
%%% the same data present in the version 1 UUID
%%% (e.g., "Version 6" at http://gh.peabody.io/uuidv6/).
%%% @end
%%%
%%% MIT License
%%%
%%% Copyright (c) 2011-2022 Michael Truog <mjtruog at protonmail dot com>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a
%%% copy of this software and associated documentation files (the "Software"),
%%% to deal in the Software without restriction, including without limitation
%%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%%% and/or sell copies of the Software, and to permit persons to whom the
%%% Software is furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in
%%% all copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%%% DEALINGS IN THE SOFTWARE.
%%% @end
%%% Created : 26. 6月 2022 上午12:12
%%%-------------------------------------------------------------------
-module(alinkutil_uuid).
-author("yqfclid").



-define(UUID_NAMESPACE_DNS,  <<16#6ba7b8109dad11d180b400c04fd430c8:128>>).
-define(UUID_NAMESPACE_URL,  <<16#6ba7b8119dad11d180b400c04fd430c8:128>>).
-define(UUID_NAMESPACE_OID,  <<16#6ba7b8129dad11d180b400c04fd430c8:128>>).
-define(UUID_NAMESPACE_X500, <<16#6ba7b8149dad11d180b400c04fd430c8:128>>).

%% external interface
-export([new/1,
    new/2,
    get_v1/1,
    get_v1_time/0,
    get_v1_time/1,
    get_v1_datetime/1,
    get_v1_datetime/2,
    is_v1/1,
    get_v3/1,
    get_v3/2,
    get_v3_compat/1,
    get_v3_compat/2,
    is_v3/1,
    get_v4/0,
    get_v4/1,
    get_v4_urandom/0,
    is_v4/1,
    get_v5/1,
    get_v5/2,
    get_v5_compat/1,
    get_v5_compat/2,
    is_v5/1,
    uuid_to_list/1,
    uuid_to_string/1,
    uuid_to_string/2,
    string_to_uuid/1,
    is_uuid/1,
    increment/1,
    mac_address/0,
    test/0]).

-ifdef(ERLANG_OTP_VERSION_16).
-else.
-ifdef(ERLANG_OTP_VERSION_17).
-else.
-define(ERLANG_OTP_VERSION_18_FEATURES, true).
-ifdef(ERLANG_OTP_VERSION_18).
-else.
-define(ERLANG_OTP_VERSION_19_FEATURES, true).
-ifdef(ERLANG_OTP_VERSION_19).
-else.
-define(ERLANG_OTP_VERSION_20_FEATURES, true).
-ifdef(ERLANG_OTP_VERSION_20).
-else.
-ifdef(OTP_RELEASE). % Erlang/OTP >= 21.0
% able to use -elif here
-else.
-error("Erlang/OTP version invalid").
-endif.
-endif.
-endif.
-endif.
-endif.
-endif.

-ifdef(ERLANG_OTP_VERSION_18_FEATURES).
-type timestamp_type_internal() :: 'erlang_timestamp' | 'os' | 'warp'.
-else.
-type timestamp_type_internal() :: 'erlang_now' | 'os'.
-endif.
-type v1_variant() :: 'rfc4122' | 'ordered'.

-record(uuid_state,
{
    variant :: v1_variant(),
    node_id :: <<_:48>>,
    clock_seq :: 0..16383,
    timestamp_type :: timestamp_type_internal(),
    timestamp_last :: integer() % microseconds
}).

-type uuid() :: <<_:128>>.
-type timestamp_type() :: 'erlang' | 'os' | 'warp'.
-type state() :: #uuid_state{}.
-export_type([uuid/0,
    timestamp_type/0,
    state/0]).

-ifdef(ERLANG_OTP_VERSION_19_FEATURES).
-type iso8601() ::
nonempty_list($0..$9 | $T | $- | $: | $. | $Z).
-type uuid_string_list() ::
nonempty_list($0..$9 | $a..$f | $-).
-else.
-type iso8601() ::
nonempty_string().
-type uuid_string_list() ::
nonempty_string().
-endif.
-type uuid_string_binary() ::
<<_:256>> | <<_:288>>.
-type uuid_string() :: uuid_string_list() | uuid_string_binary().
-export_type([iso8601/0,
    uuid_string_list/0,
    uuid_string_binary/0,
    uuid_string/0]).

% Erlang Binary Term Format constants
% info from http://erlang.org/doc/apps/erts/erl_ext_dist.html
-define(TAG_VERSION, 131).
-define(TAG_PID_EXT, 103).
-define(TAG_NEW_PID_EXT, 88).

% 16#01b21dd213814000 is the number of 100-ns intervals between the
% UUID epoch 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

%%%------------------------------------------------------------------------
%%% External interface functions
%%%------------------------------------------------------------------------

%%-------------------------------------------------------------------------
%% @doc
%% ===Create new UUID state for v1 UUID generation.===
%% @end
%%-------------------------------------------------------------------------

-spec new(Pid :: pid()) ->
    state().

new(Pid) when is_pid(Pid) ->
    new(Pid, [{timestamp_type, erlang}]).

%%-------------------------------------------------------------------------
%% @doc
%% ===Create new UUID state for v1 UUID generation using a specific type of timestamp.===
%% The timestamp can either be based on erlang's adjustment of time
%% (for only strictly monotonically increasing time values) or the
%% operating system's time without any adjustment
%% (with timestamp_type values `erlang' and `os', respectively).
%% If you want erlang's adjustment of time without enforcement of increasing
%% time values, use the `warp' timestamp_type value with Erlang >= 18.0.
%% @end
%%-------------------------------------------------------------------------

-spec new(Pid :: pid(),
    Options :: timestamp_type() |
    list({timestamp_type, timestamp_type()} |
    {mac_address, list(non_neg_integer())} |
    {variant, v1_variant()})) ->
    state().

new(Pid, TimestampType)
    when is_pid(Pid),
    ((TimestampType =:= erlang) orelse
        (TimestampType =:= os) orelse
        (TimestampType =:= warp)) ->
    new(Pid, [{timestamp_type, TimestampType}]);
new(Pid, Options)
    when is_pid(Pid), is_list(Options) ->
    TimestampType = case lists:keyfind(timestamp_type, 1, Options) of
                        {timestamp_type, Value1} ->
                            Value1;
                        false ->
                            erlang
                    end,
    MacAddress = case lists:keyfind(mac_address, 1, Options) of
                     {mac_address, Value2} ->
                         Value2;
                     false ->
                         mac_address()
                 end,
    Variant = case lists:keyfind(variant, 1, Options) of
                  {variant, Value3} ->
                      Value3;
                  false ->
                      rfc4122
              end,

    % make the version 1 UUID specific to the Erlang node and pid

    % 48 bits for the first MAC address found is included with the
    % distributed Erlang node name to be hashed with the node creation count
    NodeData = [MacAddress, erlang:atom_to_binary(node(), utf8)],

    % Reduce the node information to 16 bits and the pid information to 32 bits
    % for use as the UUID v1 Node Id
    {NodeCreationCount, PidData} = case erlang:term_to_binary(Pid) of
                                       % (when the pid format changes, handle the different formats)
                                       <<?TAG_VERSION:8,
                                           ?TAG_PID_EXT:8,PidBin/binary>> ->
                                           % 72 bits for the Erlang pid
                                           <<PidID1:8,PidID2:8,PidID3:8,PidID4:8,% ID (Node specific, 15 bits)
                                               PidSR1:8,PidSR2:8,PidSR3:8,PidSR4:8,% Serial (extra uniqueness)
                                               PidCR:8                             % Node Creation Count
                                           >> = binary:part(PidBin, erlang:byte_size(PidBin), -9),
                                           {PidCR,
                                               [PidID1, PidID2, PidID3, PidID4,
                                                   PidSR1, PidSR2, PidSR3, PidSR4]};
                                       % format supported in Erlang/OTP 19.0-rc1
                                       % required for Erlang/OTP 23.0 (and Erlang/OTP 22.0-rc2)
                                       <<?TAG_VERSION:8,
                                           ?TAG_NEW_PID_EXT:8,PidBin/binary>> ->
                                           % 96 bits for the Erlang pid
                                           <<PidID1:8,PidID2:8,PidID3:8,PidID4:8,% ID (Node specific, 15 bits)
                                               PidSR1:8,PidSR2:8,PidSR3:8,PidSR4:8,% Serial (extra uniqueness)
                                               PidCR:32                            % Node Creation Count
                                           >> = binary:part(PidBin, erlang:byte_size(PidBin), -12),
                                           {PidCR,
                                               [PidID1, PidID2, PidID3, PidID4,
                                                   PidSR1, PidSR2, PidSR3, PidSR4]}
                                   end,
    Node32 = quickrand_hash:jenkins_32(NodeData, NodeCreationCount),
    Node16 = (Node32 bsr 16) bxor (Node32 band 16#FFFF),
    Pid32 = quickrand_hash:jenkins_32(PidData),
    NodeId = <<Node16:16/big-unsigned-integer,
        Pid32:32/big-unsigned-integer>>,

    ClockSeq = if
                   Variant =:= rfc4122 ->
                       pseudo_random(16384) - 1;
                   Variant =:= ordered ->
                       pseudo_random(8192) - 1
               end,
    TimestampTypeInternal = if
                                TimestampType =:= os ->
                                    os;
                                TimestampType =:= erlang ->
                                    timestamp_type_erlang();
                                TimestampType =:= warp ->
                                    case erlang:function_exported(erlang, system_time, 0) of
                                        true ->
                                            % Erlang >= 18.0
                                            warp;
                                        false ->
                                            % Erlang < 18.0
                                            erlang:exit(badarg)
                                    end
                            end,
    TimestampLast = timestamp(TimestampTypeInternal),
    #uuid_state{variant = Variant,
        node_id = NodeId,
        clock_seq = ClockSeq,
        timestamp_type = TimestampTypeInternal,
        timestamp_last = TimestampLast}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v1 UUID.===
%% @end
%%-------------------------------------------------------------------------

-spec get_v1(State :: state()) ->
    {uuid(), StateNew :: state()}.

get_v1(#uuid_state{variant = rfc4122,
    node_id = NodeId,
    clock_seq = ClockSeq,
    timestamp_type = TimestampTypeInternal,
    timestamp_last = TimestampLast} = State) ->
    MicroSeconds = timestamp(TimestampTypeInternal, TimestampLast),
    Time = MicroSeconds * 10 + ?GREGORIAN_EPOCH_OFFSET,
    % will be larger than 60 bits after 5236-03-31 21:21:00
    <<TimeHigh:12, TimeMid:16, TimeLow:32>> = <<Time:60>>,
    {<<TimeLow:32, TimeMid:16,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        TimeHigh:12,
        1:1, 0:1,            % RFC 4122 variant bits
        ClockSeq:14,
        NodeId/binary>>,
        State#uuid_state{timestamp_last = MicroSeconds}};
get_v1(#uuid_state{variant = ordered,
    node_id = NodeId,
    clock_seq = ClockSeq,
    timestamp_type = TimestampTypeInternal,
    timestamp_last = TimestampLast} = State) ->
    MicroSeconds = timestamp(TimestampTypeInternal, TimestampLast),
    Time = MicroSeconds * 10 + ?GREGORIAN_EPOCH_OFFSET,
    % will be larger than 60 bits after 5236-03-31 21:21:00
    <<TimeHigh:48, TimeLow:12>> = <<Time:60>>,
    {<<TimeHigh:48,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        TimeLow:12,
        1:1, 1:1, 1:1,       % ordered (future definition) variant bits
        ClockSeq:13,
        NodeId/binary>>,
        State#uuid_state{timestamp_last = MicroSeconds}}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get the current time value in a manner consistent with the v1 UUID.===
%% The result is an integer in microseconds since the UNIX epoch.
%% (The UNIX epoch is 1970-01-01T00:00:00Z)
%% @end
%%-------------------------------------------------------------------------

-spec get_v1_time() ->
    non_neg_integer().

get_v1_time() ->
    get_v1_time(erlang).

%%-------------------------------------------------------------------------
%% @doc
%% ===Get the current time value in a manner consistent with the v1 UUID.===
%% The result is an integer in microseconds since the UNIX epoch.
%% (The UNIX epoch is 1970-01-01T00:00:00Z)
%% @end
%%-------------------------------------------------------------------------

-spec get_v1_time(timestamp_type() | state() | uuid()) ->
    non_neg_integer().

-ifdef(ERLANG_OTP_VERSION_18_FEATURES).
get_v1_time(erlang) ->
    timestamp(erlang_timestamp);

get_v1_time(os) ->
    timestamp(os);

get_v1_time(warp) ->
    timestamp(warp);

get_v1_time(#uuid_state{timestamp_type = TimestampTypeInternal}) ->
    timestamp(TimestampTypeInternal);

get_v1_time(Value)
    when is_binary(Value), byte_size(Value) == 16 ->
    <<Time:60>> = case Value of
                      <<TimeLow:32, TimeMid:16,
                          0:1, 0:1, 0:1, 1:1,  % version 1 bits
                          TimeHigh:12,
                          1:1, 0:1,            % RFC 4122 variant bits
                          _:14,
                          _:48>> ->
                          <<TimeHigh:12, TimeMid:16, TimeLow:32>>;
                      <<TimeHigh:48,
                          0:1, 0:1, 0:1, 1:1,  % version 1 bits
                          TimeLow:12,
                          1:1, 1:1, 1:1,       % ordered (future definition) variant bits
                          _:13,
                          _:48>> ->
                          <<TimeHigh:48, TimeLow:12>>
                  end,
    (Time - ?GREGORIAN_EPOCH_OFFSET) div 10.
-else.
get_v1_time(erlang) ->
    timestamp(erlang_now);

get_v1_time(os) ->
    timestamp(os);

get_v1_time(warp) ->
    erlang:exit(badarg);

get_v1_time(#uuid_state{timestamp_type = TimestampTypeInternal}) ->
    timestamp(TimestampTypeInternal);

get_v1_time(Value)
    when is_binary(Value), byte_size(Value) == 16 ->
    <<Time:60>> = case Value of
                      <<TimeLow:32, TimeMid:16,
                          0:1, 0:1, 0:1, 1:1,  % version 1 bits
                          TimeHigh:12,
                          1:1, 0:1,            % RFC 4122 variant bits
                          _:14,
                          _:48>> ->
                          <<TimeHigh:12, TimeMid:16, TimeLow:32>>;
                      <<TimeHigh:48,
                          0:1, 0:1, 0:1, 1:1,  % version 1 bits
                          TimeLow:12,
                          1:1, 1:1, 1:1,       % ordered (future definition) variant bits
                          _:13,
                          _:48>> ->
                          <<TimeHigh:48, TimeLow:12>>
                  end,
    (Time - ?GREGORIAN_EPOCH_OFFSET) div 10.
-endif.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get an ISO8601 datetime in UTC from a v1 UUID's time value.===
%% http://www.w3.org/TR/NOTE-datetime
%% @end
%%-------------------------------------------------------------------------

-spec get_v1_datetime(Value :: timestamp_type() | state() | uuid() |
erlang:timestamp()) ->
    iso8601().

get_v1_datetime({_, _, MicroSeconds} = Timestamp) ->
    {{DateYYYY, DateMM, DateDD},
        {TimeHH, TimeMM, TimeSS}} = calendar:now_to_universal_time(Timestamp),
    [DateYYYY0, DateYYYY1,
        DateYYYY2, DateYYYY3] = int_to_dec_list(DateYYYY, 4),
    [DateMM0, DateMM1] = int_to_dec_list(DateMM, 2),
    [DateDD0, DateDD1] = int_to_dec_list(DateDD, 2),
    [TimeHH0, TimeHH1] = int_to_dec_list(TimeHH, 2),
    [TimeMM0, TimeMM1] = int_to_dec_list(TimeMM, 2),
    [TimeSS0, TimeSS1] = int_to_dec_list(TimeSS, 2),
    [MicroSeconds0, MicroSeconds1,
        MicroSeconds2, MicroSeconds3,
        MicroSeconds4, MicroSeconds5] = int_to_dec_list(MicroSeconds, 6),
    [DateYYYY0, DateYYYY1, DateYYYY2, DateYYYY3, $-,
        DateMM0, DateMM1, $-, DateDD0, DateDD1, $T,
        TimeHH0, TimeHH1, $:, TimeMM0, TimeMM1, $:, TimeSS0, TimeSS1, $.,
        MicroSeconds0, MicroSeconds1,
        MicroSeconds2, MicroSeconds3,
        MicroSeconds4, MicroSeconds5, $Z];
get_v1_datetime(Value) ->
    get_v1_datetime(Value, 0).

%%-------------------------------------------------------------------------
%% @doc
%% ===Get an ISO8601 datetime in UTC from a v1 UUID's time value with an offset in microseconds.===
%% http://www.w3.org/TR/NOTE-datetime
%% @end
%%-------------------------------------------------------------------------

-spec get_v1_datetime(Value :: timestamp_type() | state() | uuid() |
erlang:timestamp(),
    MicroSecondsOffset :: integer()) ->
    iso8601().

get_v1_datetime(Value, MicroSecondsOffset)
    when is_integer(MicroSecondsOffset) ->
    MicroSecondsTotal = get_v1_time(Value) + MicroSecondsOffset,
    MegaSeconds = MicroSecondsTotal div 1000000000000,
    Seconds = (MicroSecondsTotal div 1000000) - MegaSeconds * 1000000,
    MicroSeconds = MicroSecondsTotal rem 1000000,
    get_v1_datetime({MegaSeconds, Seconds, MicroSeconds}).

%%-------------------------------------------------------------------------
%% @doc
%% ===Is the binary a v1 UUID?===
%% @end
%%-------------------------------------------------------------------------

-spec is_v1(Value :: any()) ->
    boolean().

is_v1(<<_TimeLow:32, _TimeMid:16,
    0:1, 0:1, 0:1, 1:1,  % version 1 bits
    _TimeHigh:12,
    1:1, 0:1,            % RFC 4122 variant bits
    _ClockSeq:14,
    _NodeId:48>>) ->
    true;
is_v1(<<_TimeHigh:48,
    0:1, 0:1, 0:1, 1:1,  % version 1 bits
    _TimeLow:12,
    1:1, 1:1, 1:1,       % ordered (future definition) variant bits
    _ClockSeq:13,
    _NodeId:48>>) ->
    true;
is_v1(_) ->
    false.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v3 UUID.===
%% @end
%%-------------------------------------------------------------------------

-spec get_v3(Data :: binary()) ->
    uuid().

get_v3(Data) when is_binary(Data) ->
    <<B1:48, B4a:4, B2:12, B4b:2, B3:14, B4c:48>> =
        crypto:hash(md5, Data),
    B4 = (B4a bxor B4b) bxor B4c,
    <<B1:48,
        0:1, 0:1, 1:1, 1:1,  % version 3 bits
        B2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        B3:14,
        B4:48>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v3 UUID in a particular namespace.===
%% @end
%%-------------------------------------------------------------------------

-spec get_v3(Namespace :: dns | url | oid | x500 | binary(),
    Data :: iodata()) ->
    uuid().

get_v3(dns, Data) ->
    get_v3(?UUID_NAMESPACE_DNS, Data);
get_v3(url, Data) ->
    get_v3(?UUID_NAMESPACE_URL, Data);
get_v3(oid, Data) ->
    get_v3(?UUID_NAMESPACE_OID, Data);
get_v3(x500, Data) ->
    get_v3(?UUID_NAMESPACE_X500, Data);
get_v3(Namespace, Data) when is_binary(Namespace) ->
    get_v3(erlang:iolist_to_binary([Namespace, Data])).

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a compatible v3 UUID.===
%% Do not use all bits from the checksum so that the UUID matches external
%% implementations.
%% @end
%%-------------------------------------------------------------------------

-spec get_v3_compat(Data :: binary()) ->
    uuid().

get_v3_compat(Data) when is_binary(Data) ->
    <<B1:48, _:4, B2:12, _:2, B3:14, B4:48>> =
        crypto:hash(md5, Data),
    <<B1:48,
        0:1, 0:1, 1:1, 1:1,  % version 3 bits
        B2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        B3:14,
        B4:48>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a compatible v3 UUID in a particular namespace.===
%% Do not use all bits from the checksum so that the UUID matches external
%% implementations.
%% @end
%%-------------------------------------------------------------------------

-spec get_v3_compat(Namespace :: dns | url | oid | x500 | binary(),
    Data :: iodata()) ->
    uuid().

get_v3_compat(dns, Data) ->
    get_v3_compat(?UUID_NAMESPACE_DNS, Data);
get_v3_compat(url, Data) ->
    get_v3_compat(?UUID_NAMESPACE_URL, Data);
get_v3_compat(oid, Data) ->
    get_v3_compat(?UUID_NAMESPACE_OID, Data);
get_v3_compat(x500, Data) ->
    get_v3_compat(?UUID_NAMESPACE_X500, Data);
get_v3_compat(Namespace, Data) when is_binary(Namespace) ->
    get_v3_compat(erlang:iolist_to_binary([Namespace, Data])).

%%-------------------------------------------------------------------------
%% @doc
%% ===Is the binary a v3 UUID?===
%% @end
%%-------------------------------------------------------------------------

-spec is_v3(Value :: any()) ->
    boolean().

is_v3(<<_:48,
    0:1, 0:1, 1:1, 1:1,  % version 3 bits
    _:12,
    1:1, 0:1,            % RFC 4122 variant bits
    _:62>>) ->
    true;
is_v3(_) ->
    false.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v4 UUID (using crypto/openssl).===
%% crypto:strong_rand_bytes/1 repeats in the same way as
%% RAND_bytes within OpenSSL.
%% @end
%%-------------------------------------------------------------------------

-spec get_v4() ->
    uuid().

get_v4() ->
    get_v4(strong).

-spec get_v4('strong' | 'cached' | quickrand_cache:state()) ->
    uuid() | {uuid(), quickrand_cache:state()}.

get_v4(strong) ->
    <<Rand1:48, _:4, Rand2:12, _:2, Rand3:62>> =
        crypto:strong_rand_bytes(16),
    <<Rand1:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        Rand2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        Rand3:62>>;
get_v4(cached) ->
    <<Rand1:48, _:4, Rand2:12, _:2, Rand3:62>> =
        quickrand_cache:rand_bytes(16),
    <<Rand1:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        Rand2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        Rand3:62>>;
get_v4(Cache) when element(1, Cache) =:= quickrand_cache ->
    {<<Rand1:48, _:4, Rand2:12, _:2, Rand3:62>>, CacheNew} =
        quickrand_cache:rand_bytes(16, Cache),
    {<<Rand1:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        Rand2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        Rand3:62>>, CacheNew}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v4 UUID (using a 256-bit Marsaglia multiply-with-carry PRNG).===
%% quickrand:mwc256_128/1 repeats every 5.79e76 (2^255) approx.
%% (see quickrand.erl for details)
%% @end
%%-------------------------------------------------------------------------

-spec get_v4_urandom() ->
    uuid().

get_v4_urandom() ->
    % random 122 bits
    Rand = quickrand:mwc256_128(16#4000000000000000000000000000000) - 1,
    <<Rand1:48, Rand2:12, Rand3:62>> = <<Rand:122>>,
    <<Rand1:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        Rand2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        Rand3:62>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Is the binary a v4 UUID?===
%% @end
%%-------------------------------------------------------------------------

-spec is_v4(Value :: any()) ->
    boolean().

is_v4(<<_:48,
    0:1, 1:1, 0:1, 0:1,  % version 4 bits
    _:12,
    1:1, 0:1,            % RFC 4122 variant bits
    _:62>>) ->
    true;
is_v4(_) ->
    false.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v5 UUID.===
%% @end
%%-------------------------------------------------------------------------

-spec get_v5(Data :: binary()) ->
    uuid().

get_v5(Data) when is_binary(Data) ->
    <<B1:48, B4a:4, B2:12, B4b:2, B3:14, B4c:32, B4d:48>> =
        crypto:hash(sha, Data),
    B4 = ((B4a bxor B4b) bxor B4c) bxor B4d,
    <<B1:48,
        0:1, 1:1, 0:1, 1:1,  % version 5 bits
        B2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        B3:14,
        B4:48>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a v5 UUID in a particular namespace.===
%% @end
%%-------------------------------------------------------------------------

-spec get_v5(Namespace :: dns | url | oid | x500 | binary(),
    Data :: iodata()) ->
    uuid().

get_v5(dns, Data) ->
    get_v5(?UUID_NAMESPACE_DNS, Data);
get_v5(url, Data) ->
    get_v5(?UUID_NAMESPACE_URL, Data);
get_v5(oid, Data) ->
    get_v5(?UUID_NAMESPACE_OID, Data);
get_v5(x500, Data) ->
    get_v5(?UUID_NAMESPACE_X500, Data);
get_v5(Namespace, Data) when is_binary(Namespace) ->
    get_v5(erlang:iolist_to_binary([Namespace, Data])).

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a compatible v5 UUID.===
%% Do not use all bits from the checksum so that the UUID matches external
%% implementations.
%% @end
%%-------------------------------------------------------------------------

-spec get_v5_compat(Data :: binary()) ->
    uuid().

get_v5_compat(Data) when is_binary(Data) ->
    <<B1:48, _:4, B2:12, _:2, B3:14, B4:48, _:32>> =
        crypto:hash(sha, Data),
    <<B1:48,
        0:1, 1:1, 0:1, 1:1,  % version 5 bits
        B2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        B3:14,
        B4:48>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a compatible v5 UUID in a particular namespace.===
%% Do not use all bits from the checksum so that the UUID matches external
%% implementations.
%% @end
%%-------------------------------------------------------------------------

-spec get_v5_compat(Namespace :: dns | url | oid | x500 | binary(),
    Data :: iodata()) ->
    uuid().

get_v5_compat(dns, Data) ->
    get_v5_compat(?UUID_NAMESPACE_DNS, Data);
get_v5_compat(url, Data) ->
    get_v5_compat(?UUID_NAMESPACE_URL, Data);
get_v5_compat(oid, Data) ->
    get_v5_compat(?UUID_NAMESPACE_OID, Data);
get_v5_compat(x500, Data) ->
    get_v5_compat(?UUID_NAMESPACE_X500, Data);
get_v5_compat(Namespace, Data) when is_binary(Namespace) ->
    get_v5_compat(erlang:iolist_to_binary([Namespace, Data])).

%%-------------------------------------------------------------------------
%% @doc
%% ===Is the binary a v5 UUID?===
%% @end
%%-------------------------------------------------------------------------

-spec is_v5(Value :: any()) ->
    boolean().

is_v5(<<_:48,
    0:1, 1:1, 0:1, 1:1,  % version 5 bits
    _:12,
    1:1, 0:1,            % RFC 4122 variant bits
    _:62>>) ->
    true;
is_v5(_) ->
    false.

%%-------------------------------------------------------------------------
%% @doc
%% ===Convert a UUID to a list.===
%% @end
%%-------------------------------------------------------------------------

-spec uuid_to_list(uuid()) ->
    nonempty_list(non_neg_integer()).

uuid_to_list(<<B1:32/unsigned-integer,
    B2:16/unsigned-integer,
    B3:16/unsigned-integer,
    B4:16/unsigned-integer,
    B5:48/unsigned-integer>>) ->
    [B1, B2, B3, B4, B5].

%%-------------------------------------------------------------------------
%% @doc
%% ===Convert a UUID to a string representation.===
%% @end
%%-------------------------------------------------------------------------

-spec uuid_to_string(Value :: uuid()) ->
    uuid_string_list().

uuid_to_string(Value) ->
    uuid_to_string(Value, standard).

%%-------------------------------------------------------------------------
%% @doc
%% ===Convert a UUID to a string representation based on an option.===
%% @end
%%-------------------------------------------------------------------------

-spec uuid_to_string(Value :: uuid(),
    Option :: standard | nodash |
    list_standard | list_nodash |
    binary_standard | binary_nodash) ->
    uuid_string().

uuid_to_string(<<Value:128/unsigned-integer>>, standard) ->
    [N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32] =
        int_to_hex_list(Value, 32),
    [N01, N02, N03, N04, N05, N06, N07, N08, $-,
        N09, N10, N11, N12, $-,
        N13, N14, N15, N16, $-,
        N17, N18, N19, N20, $-,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32];

uuid_to_string(<<Value:128/unsigned-integer>>, nodash) ->
    int_to_hex_list(Value, 32);

uuid_to_string(Value, list_standard) ->
    uuid_to_string(Value, standard);

uuid_to_string(Value, list_nodash) ->
    uuid_to_string(Value, nodash);

uuid_to_string(<<Value:128/unsigned-integer>>, binary_standard) ->
    [N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32] =
        int_to_hex_list(Value, 32),
    <<N01, N02, N03, N04, N05, N06, N07, N08, $-,
        N09, N10, N11, N12, $-,
        N13, N14, N15, N16, $-,
        N17, N18, N19, N20, $-,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32>>;

uuid_to_string(<<Value:128/unsigned-integer>>, binary_nodash) ->
    [N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32] =
        int_to_hex_list(Value, 32),
    <<N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26, N27, N28, N29, N30, N31, N32>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Convert a string representation to a UUID.===
%% @end
%%-------------------------------------------------------------------------

-spec string_to_uuid(uuid_string()) ->
    uuid().

string_to_uuid([N01, N02, N03, N04, N05, N06, N07, N08, $-,
    N09, N10, N11, N12, $-,
    N13, N14, N15, N16, $-,
    N17, N18, N19, N20, $-,
    N21, N22, N23, N24, N25, N26,
    N27, N28, N29, N30, N31, N32]) ->
    string_to_uuid(N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26,
        N27, N28, N29, N30, N31, N32);

string_to_uuid([N01, N02, N03, N04, N05, N06, N07, N08,
    N09, N10, N11, N12,
    N13, N14, N15, N16,
    N17, N18, N19, N20,
    N21, N22, N23, N24, N25, N26,
    N27, N28, N29, N30, N31, N32]) ->
    string_to_uuid(N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26,
        N27, N28, N29, N30, N31, N32);

string_to_uuid(<<N01, N02, N03, N04, N05, N06, N07, N08, $-,
    N09, N10, N11, N12, $-,
    N13, N14, N15, N16, $-,
    N17, N18, N19, N20, $-,
    N21, N22, N23, N24, N25, N26,
    N27, N28, N29, N30, N31, N32>>) ->
    string_to_uuid(N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26,
        N27, N28, N29, N30, N31, N32);

string_to_uuid(<<N01, N02, N03, N04, N05, N06, N07, N08,
    N09, N10, N11, N12,
    N13, N14, N15, N16,
    N17, N18, N19, N20,
    N21, N22, N23, N24, N25, N26,
    N27, N28, N29, N30, N31, N32>>) ->
    string_to_uuid(N01, N02, N03, N04, N05, N06, N07, N08,
        N09, N10, N11, N12,
        N13, N14, N15, N16,
        N17, N18, N19, N20,
        N21, N22, N23, N24, N25, N26,
        N27, N28, N29, N30, N31, N32);

string_to_uuid(_) ->
    erlang:exit(badarg).

string_to_uuid(N01, N02, N03, N04, N05, N06, N07, N08,
    N09, N10, N11, N12,
    N13, N14, N15, N16,
    N17, N18, N19, N20,
    N21, N22, N23, N24, N25, N26,
    N27, N28, N29, N30, N31, N32) ->
    B01 = hex_to_int(N01, N02),
    B02 = hex_to_int(N03, N04),
    B03 = hex_to_int(N05, N06),
    B04 = hex_to_int(N07, N08),
    B05 = hex_to_int(N09, N10),
    B06 = hex_to_int(N11, N12),
    B07 = hex_to_int(N13, N14),
    B08 = hex_to_int(N15, N16),
    B09 = hex_to_int(N17, N18),
    B10 = hex_to_int(N19, N20),
    B11 = hex_to_int(N21, N22),
    B12 = hex_to_int(N23, N24),
    B13 = hex_to_int(N25, N26),
    B14 = hex_to_int(N27, N28),
    B15 = hex_to_int(N29, N30),
    B16 = hex_to_int(N31, N32),
    <<B01, B02, B03, B04, B05, B06, B07, B08,
        B09, B10, B11, B12, B13, B14, B15, B16>>.

%%-------------------------------------------------------------------------
%% @doc
%% ===Is the term a UUID?===
%% @end
%%-------------------------------------------------------------------------

-spec is_uuid(any()) ->
    boolean().

is_uuid(<<0:128>>) ->
    true;
is_uuid(<<_:48,
    Version:4/unsigned-integer,
    _:12,
    1:1, 0:1,            % RFC 4122 variant bits
    _:62>>) ->
    (Version == 1) orelse
        (Version == 3) orelse
        (Version == 4) orelse
        (Version == 5);
is_uuid(<<_:48,
    Version:4/unsigned-integer,
    _:12,
    1:1, 1:1, 1:1,       % ordered (future definition) variant bits
    _:61>>) ->
    (Version == 1);
is_uuid(_) ->
    false.

%%-------------------------------------------------------------------------
%% @doc
%% ===Increment the clock sequence of v1 UUID state or a UUID.===
%% Call to increment the clock sequence counter after the system clock has
%% been set backwards (see the RFC).  This is only necessary
%% if the `os' or `warp' timestamp_type is used with a v1 UUID.
%% The v3, v4 and v5 UUIDs are supported for completeness.
%% @end
%%-------------------------------------------------------------------------

-spec increment(state() | uuid()) ->
    state() | uuid().

increment(<<TimeLow:32, TimeMid:16,
    0:1, 0:1, 0:1, 1:1,  % version 1 bits
    TimeHigh:12,
    1:1, 0:1,            % RFC 4122 variant bits
    ClockSeq:14,
    NodeId:48>>) ->
    ClockSeqNext = ClockSeq + 1,
    ClockSeqNew = if
                      ClockSeqNext == 16384 ->
                          0;
                      true ->
                          ClockSeqNext
                  end,
    <<TimeLow:32, TimeMid:16,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        TimeHigh:12,
        1:1, 0:1,            % RFC 4122 variant bits
        ClockSeqNew:14,
        NodeId:48>>;
increment(<<TimeHigh:48,
    0:1, 0:1, 0:1, 1:1,  % version 1 bits
    TimeLow:12,
    1:1, 1:1, 1:1,       % ordered (future definition) variant bits
    ClockSeq:13,
    NodeId:48>>) ->
    ClockSeqNext = ClockSeq + 1,
    ClockSeqNew = if
                      ClockSeqNext == 8192 ->
                          0;
                      true ->
                          ClockSeqNext
                  end,
    <<TimeHigh:48,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        TimeLow:12,
        1:1, 1:1, 1:1,       % ordered (future definition) variant bits
        ClockSeqNew:13,
        NodeId:48>>;
increment(<<Rand1:48,
    Version:4/unsigned-integer,
    Rand2:12,
    1:1, 0:1,            % RFC 4122 variant bits
    Rand3:62>>)
    when (Version == 4) orelse (Version == 5) orelse (Version == 3) ->
    <<Value:16/little-unsigned-integer-unit:8>> = <<Rand1:48,
        Rand2:12,
        Rand3:62,
        0:6>>,
    ValueNext = Value + 1,
    ValueNew = if
                   ValueNext == 5316911983139663491615228241121378304 ->
                       1;
                   true ->
                       ValueNext
               end,
    <<Rand1New:48,
        Rand2New:12,
        Rand3New:62,
        0:6>> = <<ValueNew:16/little-unsigned-integer-unit:8>>,
    <<Rand1New:48,
        Version:4/unsigned-integer,
        Rand2New:12,
        1:1, 0:1,            % RFC 4122 variant bits
        Rand3New:62>>;
increment(#uuid_state{variant = Variant,
    clock_seq = ClockSeq} = State) ->
    ClockSeqNext = ClockSeq + 1,
    ClockSeqNew = if
                      (Variant =:= rfc4122) andalso (ClockSeqNext == 16384);
                      (Variant =:= ordered) andalso (ClockSeqNext == 8192) ->
                          0;
                      true ->
                          ClockSeqNext
                  end,
    State#uuid_state{clock_seq = ClockSeqNew}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Provide a usable network interface MAC address.===
%% @end
%%-------------------------------------------------------------------------

-spec mac_address() ->
    nonempty_list(non_neg_integer()).

mac_address() ->
    {ok, Ifs} = inet:getifaddrs(),
    mac_address(lists:keysort(1, Ifs)).

%%-------------------------------------------------------------------------
%% @doc
%% ===Regression test.===
%% @end
%%-------------------------------------------------------------------------

-spec test() ->
    ok.

test() ->
    true = uuid:is_uuid(<<0:128>>),
    % version 1 tests
    % uuidgen -t ; date
    % "Fri Dec  7 19:13:58 PST 2012"
    % (Sat Dec  8 03:13:58 UTC 2012)
    V1uuid1 = uuid:string_to_uuid("4ea4b020-40e5-11e2-ac70-001fd0a5484e"),
    "4ea4b020-40e5-11e2-ac70-001fd0a5484e" =
        uuid:uuid_to_string(V1uuid1, standard),
    <<V1TimeLow1:32, V1TimeMid1:16,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        V1TimeHigh1:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V1ClockSeq1:14,
        V1NodeId1/binary>> = V1uuid1,
    true = uuid:is_uuid(V1uuid1),
    true = uuid:is_v1(V1uuid1),
    "2012-12-08T03:13:58.564048Z" = uuid:get_v1_datetime(V1uuid1),
    <<V1Time1:60>> = <<V1TimeHigh1:12, V1TimeMid1:16, V1TimeLow1:32>>,
    V1Time1total = erlang:trunc((V1Time1 - 16#01b21dd213814000) / 10),
    V1Time1mega = erlang:trunc(V1Time1total / 1000000000000),
    V1Time1sec = erlang:trunc(V1Time1total / 1000000 - V1Time1mega * 1000000),
    V1Time1micro = erlang:trunc(V1Time1total -
        (V1Time1mega * 1000000 + V1Time1sec) * 1000000),
    {{2012, 12, 8}, {3, 13, 58}} =
        calendar:now_to_datetime({V1Time1mega, V1Time1sec, V1Time1micro}),
    % max version 1 timestamp:
    "5236-03-31T21:21:00.684697Z" = uuid:get_v1_datetime(<<16#ffffffff:32,
        16#ffff:16,
        0:1, 0:1, 0:1, 1:1,
        16#fff:12,
        1:1, 0:1,
        0:14, 0:48>>),
    % $ python
    % >>> import uuid
    % >>> import time
    % >>> uuid.uuid1().hex;time.time()
    % '50d15f5c40e911e2a0eb001fd0a5484e'
    % 1354938160.1998589
    V1uuid2 = uuid:string_to_uuid("50d15f5c40e911e2a0eb001fd0a5484e"),
    "50d15f5c40e911e2a0eb001fd0a5484e" =
        uuid:uuid_to_string(V1uuid2, nodash),
    "2012-12-08T03:42:40.199254Z" = uuid:get_v1_datetime(V1uuid2),
    <<V1TimeLow2:32, V1TimeMid2:16,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        V1TimeHigh2:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V1ClockSeq2:14,
        V1NodeId2/binary>> = V1uuid2,
    true = uuid:is_v1(V1uuid2),
    <<V1Time2:60>> = <<V1TimeHigh2:12, V1TimeMid2:16, V1TimeLow2:32>>,
    V1Time2total = erlang:trunc((V1Time2 - 16#01b21dd213814000) / 10),
    V1Time2Amega = erlang:trunc(V1Time2total / 1000000000000),
    V1Time2Asec = erlang:trunc(V1Time2total / 1000000 - V1Time2Amega * 1000000),
    V1Time2Amicro = erlang:trunc(V1Time2total -
        (V1Time2Amega * 1000000 + V1Time2Asec) * 1000000),
    V1Time2B = 1354938160.1998589,
    V1Time2Bmega = erlang:trunc(V1Time2B / 1000000),
    V1Time2Bsec = erlang:trunc(V1Time2B - V1Time2Bmega * 1000000),
    V1Time2Bmicro = erlang:trunc(V1Time2B * 1000000 -
        (V1Time2Bmega * 1000000 + V1Time2Bsec) * 1000000),
    true = (V1Time2Amega == V1Time2Bmega),
    true = (V1Time2Asec == V1Time2Bsec),
    true = (V1Time2Amicro < V1Time2Bmicro) and
        ((V1Time2Amicro + 605) == V1Time2Bmicro),
    true = V1ClockSeq1 /= V1ClockSeq2,
    true = V1NodeId1 == V1NodeId2,
    {V1uuid3, _} = uuid:get_v1(uuid:new(self(), erlang)),
    V1uuid3timeB = uuid:get_v1_time(erlang),
    V1uuid3timeA = uuid:get_v1_time(V1uuid3),
    true = (V1uuid3timeA < V1uuid3timeB) and
        ((V1uuid3timeA + 1000) > V1uuid3timeB),
    true = is_number(uuid:get_v1_time(
        uuid:string_to_uuid("3ff0fc1e-c23b-11e2-b8a0-38607751fca5"))),
    true = is_number(uuid:get_v1_time(
        uuid:string_to_uuid("67ff79a6-c23b-11e2-b374-38607751fca5"))),
    true = is_number(uuid:get_v1_time(
        uuid:string_to_uuid("7134eede-c23b-11e2-a4a7-38607751fca5"))),
    true = is_number(uuid:get_v1_time(
        uuid:string_to_uuid("717003c0-c23b-11e2-83a4-38607751fca5"))),
    {V1uuid4, _} = uuid:get_v1(uuid:new(self(), os)),
    V1uuid4timeB = uuid:get_v1_time(os),
    V1uuid4timeA = uuid:get_v1_time(V1uuid4),
    true = (V1uuid4timeA < V1uuid4timeB) and
        ((V1uuid4timeA + 1000) > V1uuid4timeB),
    V1State0 = uuid:new(self()),
    {V1uuid5,  V1State1} = uuid:get_v1(V1State0),
    {V1uuid6,  V1State2} = uuid:get_v1(V1State1),
    {V1uuid7,  V1State3} = uuid:get_v1(V1State2),
    {V1uuid8,  V1State4} = uuid:get_v1(V1State3),
    {V1uuid9,  V1State5} = uuid:get_v1(V1State4),
    {V1uuid10, V1State6} = uuid:get_v1(V1State5),
    {V1uuid11, _} = uuid:get_v1(V1State6),
    V1uuidL0 = [V1uuid5, V1uuid6, V1uuid7, V1uuid8,
        V1uuid9, V1uuid10, V1uuid11],
    V1uuidL1 = [V1uuid11, V1uuid9, V1uuid8, V1uuid7,
        V1uuid6, V1uuid10, V1uuid5],
    true = V1uuidL0 == lists:usort(V1uuidL1),
    % version 1 ordered variant
    V1uuidOrdered0 = uuid:string_to_uuid("1ea55423c2f912f6e847e7a9663898ef"),
    V1uuidOrdered1 = uuid:string_to_uuid("1ea554248aa31464e847e7a9663898ef"),
    true = V1uuidOrdered1 > V1uuidOrdered0,
    true = uuid:is_v1(V1uuidOrdered0),
    true = uuid:is_v1(V1uuidOrdered1),
    V1uuidOrdered2 = uuid:increment(V1uuidOrdered1),
    true = uuid:is_v1(V1uuidOrdered2),
    <<_:48,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        _:12,
        1:1, 1:1, 1:1,       % ordered (future definition) variant bits
        _:13,
        _:48>> = V1uuidOrdered2,
    V1OrderedState0 = uuid:new(self(), [{variant, ordered}]),
    {V1uuidOrdered3, _} = uuid:get_v1(V1OrderedState0),
    <<_:48,
        0:1, 0:1, 0:1, 1:1,  % version 1 bits
        _:12,
        1:1, 1:1, 1:1,       % ordered (future definition) variant bits
        _:13,
        _:48>> = V1uuidOrdered3,

    % version 3 tests
    % $ python
    % >>> import uuid
    % >>> uuid.uuid3(uuid.NAMESPACE_DNS, 'test').hex
    % '45a113acc7f230b090a5a399ab912716'
    V3uuid1 = uuid:string_to_uuid("45a113acc7f230b090a5a399ab912716"),
    "45a113acc7f230b090a5a399ab912716" =
        uuid:uuid_to_string(V3uuid1, nodash),
    <<V3uuid1A:48,
        0:1, 0:1, 1:1, 1:1,  % version 3 bits
        V3uuid1B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V3uuid1C:14,
        V3uuid1D:48>> = V3uuid1,
    true = uuid:is_v3(V3uuid1),
    V3uuid2 = uuid:get_v3(?UUID_NAMESPACE_DNS, <<"test">>),
    true = (V3uuid2 == uuid:get_v3(dns, <<"test">>)),
    <<V3uuid2A:48,
        0:1, 0:1, 1:1, 1:1,  % version 3 bits
        V3uuid2B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V3uuid2C:14,
        V3uuid2D:48>> = V3uuid2,
    true = uuid:is_v3(V3uuid2),
    true = ((V3uuid1A == V3uuid2A) and
        (V3uuid1B == V3uuid2B) and
        (V3uuid1C == V3uuid2C)),
    % check fails:
    % since the python uuid discards bits from MD5 while this module
    % bitwise xor the middle bits to utilize the whole checksum
    false = (V3uuid1D == V3uuid2D),

    % replicate the same UUID value used in other implementations
    % where bits are discarded from the checksum
    V3uuid1 = uuid:get_v3_compat(?UUID_NAMESPACE_DNS, <<"test">>),
    true = (uuid:get_v3_compat(dns, <<"test1">>) ==
        uuid:string_to_uuid("c501822b22a837ff91a99545f4689a3d")),
    true = (uuid:get_v3_compat(dns, <<"test2">>) ==
        uuid:string_to_uuid("f191764306b23e6dab770a5044067d0a")),
    true = (uuid:get_v3_compat(dns, <<"test3">>) ==
        uuid:string_to_uuid("bf7f1e5a6b28310c8f9ef815dbd56fb7")),
    true = (uuid:get_v3_compat(dns, <<"test4">>) ==
        uuid:string_to_uuid("fe584e2496c83d2d8b39f1cc6a877f72")),

    % version 4 tests
    % uuidgen -r
    V4uuid1 = uuid:string_to_uuid("ffe8b758-a5dc-4bf4-9eb0-878e010e8df7"),
    "ffe8b758-a5dc-4bf4-9eb0-878e010e8df7" =
        uuid:uuid_to_string(V4uuid1, standard),
    <<V4Rand1A:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        V4Rand1B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V4Rand1C:14,
        V4Rand1D:48>> = V4uuid1,
    true = uuid:is_v4(V4uuid1),
    % $ python
    % >>> import uuid
    % >>> uuid.uuid4().hex
    % 'cc9769818fe747398e2422e99fee2753'
    V4uuid2 = uuid:string_to_uuid("cc9769818fe747398e2422e99fee2753"),
    "cc9769818fe747398e2422e99fee2753" =
        uuid:uuid_to_string(V4uuid2, nodash),
    <<V4Rand2A:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        V4Rand2B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V4Rand2C:14,
        V4Rand2D:48>> = V4uuid2,
    true = uuid:is_v4(V4uuid2),
    V4uuid3 = uuid:get_v4(strong),
    <<_:48,
        0:1, 1:1, 0:1, 0:1,  % version 4 bits
        _:12,
        1:1, 0:1,            % RFC 4122 variant bits
        _:14,
        _:48>> = V4uuid3,
    true = uuid:is_v4(V4uuid3),
    true = (V4Rand1A /= V4Rand2A),
    true = (V4Rand1B /= V4Rand2B),
    true = (V4Rand1C /= V4Rand2C),
    true = (V4Rand1D /= V4Rand2D),
    true = V4uuid3 /= uuid:increment(V4uuid3),

    % version 5 tests
    % $ python
    % >>> import uuid
    % >>> uuid.uuid5(uuid.NAMESPACE_DNS, 'test').hex
    % '4be0643f1d98573b97cdca98a65347dd'
    V5uuid1 = uuid:string_to_uuid("4be0643f1d98573b97cdca98a65347dd"),
    "4be0643f1d98573b97cdca98a65347dd" =
        uuid:uuid_to_string(V5uuid1, nodash),
    <<V5uuid1A:48,
        0:1, 1:1, 0:1, 1:1,  % version 5 bits
        V5uuid1B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V5uuid1C:14,
        V5uuid1D:48>> = V5uuid1,
    true = uuid:is_v5(V5uuid1),
    V5uuid2 = uuid:get_v5(?UUID_NAMESPACE_DNS, <<"test">>),
    true = (V5uuid2 == uuid:get_v5(dns, <<"test">>)),
    <<V5uuid2A:48,
        0:1, 1:1, 0:1, 1:1,  % version 5 bits
        V5uuid2B:12,
        1:1, 0:1,            % RFC 4122 variant bits
        V5uuid2C:14,
        V5uuid2D:48>> = V5uuid2,
    true = uuid:is_v5(V5uuid2),
    true = ((V5uuid1A == V5uuid2A) and
        (V5uuid1B == V5uuid2B) and
        (V5uuid1C == V5uuid2C)),
    % check fails:
    % since the python uuid discards bits from SHA while this module
    % bitwise xor the remaining bits to utilize the whole checksum
    false = (V5uuid1D == V5uuid2D),

    % replicate the same UUID value used in other implementations
    % where bits are discarded from the checksum
    V5uuid1 = uuid:get_v5_compat(?UUID_NAMESPACE_DNS, <<"test">>),
    true = (uuid:get_v5_compat(dns, <<"test1">>) ==
        uuid:string_to_uuid("86e3aed315535d238d612286215e65f1")),
    true = (uuid:get_v5_compat(dns, <<"test2">>) ==
        uuid:string_to_uuid("6eabff02c9685cbcbc7f3b672928a761")),
    true = (uuid:get_v5_compat(dns, <<"test3">>) ==
        uuid:string_to_uuid("20ca53afd04c58a2a8b3d02b9e414e80")),
    true = (uuid:get_v5_compat(dns, <<"test4">>) ==
        uuid:string_to_uuid("3e673fdc1a4f5b168890dbe7e763f7b5")),
    ok.

%%%------------------------------------------------------------------------
%%% Private functions
%%%------------------------------------------------------------------------

-compile({inline,
    [{timestamp,1},
        {timestamp,2},
        {int_to_dec,1},
        {int_to_hex,1},
        {hex_to_int,1}]}).

-ifdef(ERLANG_OTP_VERSION_18_FEATURES).
timestamp_type_erlang() ->
    % Erlang >= 18.0
    true = erlang:function_exported(erlang, system_time, 0),
    erlang_timestamp.

-ifdef(ERLANG_OTP_VERSION_20_FEATURES).
timestamp(erlang_timestamp) ->
    erlang:system_time(microsecond);
timestamp(os) ->
    os:system_time(microsecond);
timestamp(warp) ->
    erlang:system_time(microsecond).
-else.
timestamp(erlang_timestamp) ->
    erlang:system_time(micro_seconds);
timestamp(os) ->
    os:system_time(micro_seconds);
timestamp(warp) ->
    erlang:system_time(micro_seconds).
-endif.

timestamp(erlang_timestamp, TimestampLast) ->
    TimestampNext = timestamp(erlang_timestamp),
    if
        TimestampNext > TimestampLast ->
            TimestampNext;
        true ->
            TimestampLast + 1
    end;
timestamp(os, _) ->
    timestamp(os);
timestamp(warp, _) ->
    timestamp(warp).
-else.
timestamp_type_erlang() ->
    % Erlang < 18.0
    false = erlang:function_exported(erlang, system_time, 0),
    erlang_now.

timestamp(erlang_now) ->
    {MegaSeconds, Seconds, MicroSeconds} = erlang:now(),
    (MegaSeconds * 1000000 + Seconds) * 1000000 + MicroSeconds;
timestamp(os) ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    (MegaSeconds * 1000000 + Seconds) * 1000000 + MicroSeconds.

timestamp(erlang_now, _) ->
    timestamp(erlang_now);
timestamp(os, _) ->
    timestamp(os).
-endif.

int_to_dec_list(I, N) when is_integer(I), I >= 0 ->
    int_to_dec_list([], I, 1, N).

int_to_dec_list(L, I, Count, N)
    when I < 10 ->
    int_to_list_pad([int_to_dec(I) | L], N - Count);
int_to_dec_list(L, I, Count, N) ->
    int_to_dec_list([int_to_dec(I rem 10) | L], I div 10, Count + 1, N).

int_to_hex_list(I, N) when is_integer(I), I >= 0 ->
    int_to_hex_list([], I, 1, N).

int_to_list_pad(L, 0) ->
    L;
int_to_list_pad(L, Count) ->
    int_to_list_pad([$0 | L], Count - 1).

int_to_hex_list(L, I, Count, N)
    when I < 16 ->
    int_to_list_pad([int_to_hex(I) | L], N - Count);
int_to_hex_list(L, I, Count, N) ->
    int_to_hex_list([int_to_hex(I rem 16) | L], I div 16, Count + 1, N).

int_to_dec(I) when 0 =< I, I =< 9 ->
    I + $0.

int_to_hex(I) when 0 =< I, I =< 9 ->
    I + $0;
int_to_hex(I) when 10 =< I, I =< 15 ->
    (I - 10) + $a.

hex_to_int(C1, C2) ->
    hex_to_int(C1) * 16 + hex_to_int(C2).

hex_to_int(C) when $0 =< C, C =< $9 ->
    C - $0;
hex_to_int(C) when $A =< C, C =< $F ->
    C - $A + 10;
hex_to_int(C) when $a =< C, C =< $f ->
    C - $a + 10.

mac_address([]) ->
    [0, 0, 0, 0, 0, 0];

mac_address([{_, L} | Rest]) ->
    case lists:keyfind(hwaddr, 1, L) of
        false ->
            mac_address(Rest);
        {hwaddr, MAC} ->
            case lists:sum(MAC) of
                0 ->
                    mac_address(Rest);
                _ ->
                    MAC
            end
    end.

-ifdef(ERLANG_OTP_VERSION_18_FEATURES).
pseudo_random(N) ->
    % assuming exsp/exsplus for 58 bits, period 8.31e34
    rand:uniform(N).
-else.
pseudo_random(N) ->
    % period 2.78e13
    random:uniform(N).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-ifdef(CLOUDI_TEST_TIMEOUT).
-define(TEST_TIMEOUT, ?CLOUDI_TEST_TIMEOUT). % seconds
-else.
-define(TEST_TIMEOUT, 10). % seconds
-endif.

module_test_() ->
    {timeout, ?TEST_TIMEOUT, [
        {"internal tests", ?_assertEqual(ok, test())}
    ]}.

-endif.

