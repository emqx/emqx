%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_test_utils).

-include_lib("eunit/include/eunit.hrl").

-export([setup_index_mode/1, assert_index_mode/4, buffer_max_size/0, max_kept/3]).

-type mode() :: mixed_version | fully_transitioned.

-doc "Make the BPAPI guard report an older node (mixed version) or check that it reports none".
-spec setup_index_mode(mode()) -> ok.
setup_index_mode(mixed_version) ->
    ok = emqx_mq_test_utils:stub_bpapi_supported_version(emqx_mq_consumer, 1),
    ?assertNot(emqx_mq_quota_index:is_serial_index_supported()),
    ok;
setup_index_mode(fully_transitioned) ->
    ?assert(emqx_mq_quota_index:is_serial_index_supported()),
    ok.

-doc "Verify that stored indices use the representation selected by the test group".
-spec assert_index_mode(mode(), emqx_ds:db(), map(), emqx_mq_quota_index:opts()) -> ok.
assert_index_mode(Mode, DB, #{id := Id, topic_filter := TopicFilter}, Opts) ->
    Records = emqx_ds:dirty_read(DB, [<<"topic">>, TopicFilter, Id, <<"index">>]),
    ?assertMatch([_ | _], Records),
    lists:foreach(
        fun({_, _, Bin}) ->
            Info = emqx_mq_quota_index:inspect(emqx_mq_quota_index:decode(Opts, Bin)),
            ?assertMatch(#{serial := #{records := [_ | _]}}, Info),
            assert_legacy_mode(Mode, Info)
        end,
        Records
    ).

-doc "Quota buffer size for GC tests. A small buffer keeps the overshoot of the serial index small".
-spec buffer_max_size() -> pos_integer().
buffer_max_size() ->
    10.

-doc """
Return the most data that GC may keep in one shard. `FlushAmount` is the data of one buffer flush.

* Legacy index: the limit plus one threshold (10% of the limit).
* Serial index: the limit, plus one threshold, plus one flush.
  The deadline is a bucket boundary, so up to one bucket of extra data stays.
  A full head bucket grows until the next flush closes it, so a bucket can exceed the threshold by one flush.
""".
-spec max_kept(mode(), pos_integer(), non_neg_integer()) -> pos_integer().
max_kept(mixed_version, Limit, _FlushAmount) ->
    Limit + Limit div 10;
max_kept(fully_transitioned, Limit, FlushAmount) ->
    Limit + Limit div 10 + FlushAmount.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

assert_legacy_mode(mixed_version, Info) ->
    ?assertMatch(#{legacy := #{bytes := #{}, count := #{}}}, Info);
assert_legacy_mode(fully_transitioned, Info) ->
    ?assertMatch(#{legacy := undefined}, Info).
