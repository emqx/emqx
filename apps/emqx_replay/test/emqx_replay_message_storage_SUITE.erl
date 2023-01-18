%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_replay_message_storage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").

-import(emqx_replay_message_storage, [
    make_keymapper/1,
    keymapper_info/1,
    compute_topic_bitmask/2,
    compute_time_bitmask/1,
    compute_topic_seek/4
]).

all() -> emqx_common_test_helpers:all(?MODULE).

t_make_keymapper(_) ->
    ?assertMatch(
        #{
            source := [
                {timestamp, 9, 23},
                {hash, level, 2},
                {hash, level, 4},
                {hash, levels, 8},
                {timestamp, 0, 9}
            ],
            bitsize := 46,
            epoch := 512
        },
        keymapper_info(
            make_keymapper(#{
                timestamp_bits => 32,
                topic_bits_per_level => [2, 4, 8],
                epoch => 1000
            })
        )
    ).

t_make_keymapper_single_hash_level(_) ->
    ?assertMatch(
        #{
            source := [
                {timestamp, 0, 32},
                {hash, levels, 16}
            ],
            bitsize := 48,
            epoch := 1
        },
        keymapper_info(
            make_keymapper(#{
                timestamp_bits => 32,
                topic_bits_per_level => [16],
                epoch => 1
            })
        )
    ).

t_make_keymapper_no_timestamp(_) ->
    ?assertMatch(
        #{
            source := [
                {hash, level, 4},
                {hash, level, 8},
                {hash, levels, 16}
            ],
            bitsize := 28,
            epoch := 1
        },
        keymapper_info(
            make_keymapper(#{
                timestamp_bits => 0,
                topic_bits_per_level => [4, 8, 16],
                epoch => 42
            })
        )
    ).

t_compute_topic_bitmask(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [3, 4, 5, 2], timestamp_bits => 0, epoch => 1}),
    ?assertEqual(
        2#111_1111_11111_11,
        compute_topic_bitmask([<<"foo">>, <<"bar">>], KM)
    ),
    ?assertEqual(
        2#111_0000_11111_11,
        compute_topic_bitmask([<<"foo">>, '+'], KM)
    ),
    ?assertEqual(
        2#111_0000_00000_11,
        compute_topic_bitmask([<<"foo">>, '+', '+'], KM)
    ),
    ?assertEqual(
        2#111_0000_11111_00,
        compute_topic_bitmask([<<"foo">>, '+', <<"bar">>, '+'], KM)
    ).

t_compute_topic_bitmask_wildcard(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [3, 4, 5, 2], timestamp_bits => 0, epoch => 1}),
    ?assertEqual(
        2#000_0000_00000_00,
        compute_topic_bitmask(['#'], KM)
    ),
    ?assertEqual(
        2#111_0000_00000_00,
        compute_topic_bitmask([<<"foo">>, '#'], KM)
    ),
    ?assertEqual(
        2#111_1111_11111_00,
        compute_topic_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, '#'], KM)
    ).

t_compute_topic_bitmask_wildcard_long_tail(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [3, 4, 5, 2], timestamp_bits => 0, epoch => 1}),
    ?assertEqual(
        2#111_1111_11111_11,
        compute_topic_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, <<>>, <<"xyzzy">>], KM)
    ),
    ?assertEqual(
        2#111_1111_11111_00,
        compute_topic_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, <<>>, '#'], KM)
    ).

t_compute_time_bitmask(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [1, 2, 3], timestamp_bits => 10, epoch => 200}),
    ?assertEqual(2#111_000000_1111111, compute_time_bitmask(KM)).

t_compute_time_bitmask_epoch_only(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [1, 2, 3], timestamp_bits => 10, epoch => 1}),
    ?assertEqual(2#1111111111_000000, compute_time_bitmask(KM)).

%% Filter = |123|***|678|***|
%% Mask   = |123|***|678|***|
%% Key1   = |123|011|108|121| → Seek = 0 |123|011|678|000|
%% Key2   = |123|011|679|919| → Seek = 0 |123|012|678|000|
%% Key3   = |123|999|679|001| → Seek = 1 |123|000|678|000| → eos
%% Key4   = |125|011|179|017| → Seek = 1 |123|000|678|000| → eos

t_compute_next_topic_seek(_) ->
    KM = make_keymapper(#{topic_bits_per_level => [8, 8, 16, 12], timestamp_bits => 0, epoch => 1}),
    ?assertMatch(
        none,
        compute_topic_seek(
            16#FD_42_4242_043,
            16#FD_42_4242_042,
            16#FF_FF_FFFF_FFF,
            KM
        )
    ),
    ?assertMatch(
        16#FD_11_0678_000,
        compute_topic_seek(
            16#FD_11_0108_121,
            16#FD_00_0678_000,
            16#FF_00_FFFF_000,
            KM
        )
    ),
    ?assertMatch(
        16#FD_12_0678_000,
        compute_topic_seek(
            16#FD_11_0679_919,
            16#FD_00_0678_000,
            16#FF_00_FFFF_000,
            KM
        )
    ),
    ?assertMatch(
        none,
        compute_topic_seek(
            16#FD_FF_0679_001,
            16#FD_00_0678_000,
            16#FF_00_FFFF_000,
            KM
        )
    ),
    ?assertMatch(
        none,
        compute_topic_seek(
            16#FE_11_0179_017,
            16#FD_00_0678_000,
            16#FF_00_FFFF_000,
            KM
        )
    ).
