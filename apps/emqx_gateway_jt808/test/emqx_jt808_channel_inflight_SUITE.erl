%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Test suite for JT808 channel inflight queue handling,
%% specifically testing custom msg_sn and race condition scenarios.
-module(emqx_jt808_channel_inflight_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_jt808.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% Macros from emqx_jt808_channel
-define(SN_AUTO, auto).
-define(SN_CUSTOM, custom).

%% Import functions from emqx_jt808_channel (exported via -ifdef(TEST))
-import(emqx_jt808_channel, [
    try_insert_inflight/7,
    ack_msg/4,
    set_msg_ack/2,
    get_msg_ack/2,
    custom_msg_ack_key/2,
    custom_get_msg_ack/2,
    make_test_channel/0,
    normalize_queue_item/1
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% Clean process dictionary before each test
    erase(),
    Config.

end_per_testcase(_Case, _Config) ->
    %% Clean process dictionary after each test
    erase(),
    ok.

%%--------------------------------------------------------------------
%% Test Cases for try_insert_inflight
%%--------------------------------------------------------------------

%% Test: Auto msg_sn insertion should always succeed
t_insert_auto_msg_sn(_Config) ->
    Inflight = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    Result = try_insert_inflight(MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight, Channel),

    ?assertMatch({ok, _NewInflight}, Result),
    {ok, NewInflight} = Result,

    %% Verify the key is in inflight
    AutoKey = set_msg_ack(MsgId, MsgSn),
    ?assert(emqx_inflight:contain(AutoKey, NewInflight)).

%% Test: Custom msg_sn insertion should succeed when no conflict
t_insert_custom_msg_sn_no_conflict(_Config) ->
    Inflight = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    Result = try_insert_inflight(MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight, Channel),

    ?assertMatch({ok, _NewInflight}, Result),
    {ok, NewInflight} = Result,

    %% Verify the custom key is in inflight
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    ?assert(emqx_inflight:contain(CustomKey, NewInflight)),

    %% Verify ordering was recorded as custom (sent first)
    AutoKey = set_msg_ack(MsgId, MsgSn),
    OrderKey = {msg_sn_order, AutoKey},
    ?assertEqual(?SN_CUSTOM, get(OrderKey)).

%% Test: Custom msg_sn insertion returns ok_duplicate when duplicate detected
%% The message should still be delivered (not discarded) to ensure message delivery
t_insert_custom_msg_sn_duplicate(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert first custom msg_sn
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight0, Channel
    ),

    %% Try to insert duplicate custom msg_sn - should return ok_duplicate
    %% (message will be delivered but not added to inflight again)
    Result = try_insert_inflight(MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight1, Channel),

    ?assertEqual({ok_duplicate, Inflight1}, Result).

%% Test: Race condition - auto first, then custom with same msg_sn
t_race_auto_first_then_custom(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert auto msg_sn first
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight0, Channel
    ),

    %% Insert custom msg_sn with same MsgSn
    {ok, Inflight2} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight1, Channel
    ),

    %% Verify both keys exist
    AutoKey = set_msg_ack(MsgId, MsgSn),
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    ?assert(emqx_inflight:contain(AutoKey, Inflight2)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight2)),

    %% Verify ordering was recorded as auto (auto sent first)
    OrderKey = {msg_sn_order, AutoKey},
    ?assertEqual(?SN_AUTO, get(OrderKey)).

%% Test: Custom first, then auto with same msg_sn (through separate channel usage)
t_race_custom_first_then_auto(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert custom msg_sn first
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight0, Channel
    ),

    %% Verify ordering recorded as custom
    AutoKey = set_msg_ack(MsgId, MsgSn),
    OrderKey = {msg_sn_order, AutoKey},
    ?assertEqual(?SN_CUSTOM, get(OrderKey)),

    %% Now insert auto msg_sn (simulating channel counter reached same value)
    {ok, Inflight2} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight1, Channel
    ),

    %% Verify both keys exist
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    ?assert(emqx_inflight:contain(AutoKey, Inflight2)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight2)),

    %% Ordering should still be custom (first insertion wins)
    ?assertEqual(?SN_CUSTOM, get(OrderKey)).

%%--------------------------------------------------------------------
%% Test Cases for ack_msg with race condition
%%--------------------------------------------------------------------

%% Test: ACK when only auto msg exists
t_ack_only_auto(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert auto msg_sn
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight0, Channel
    ),

    %% ACK the message
    AckMsgId = ?MC_GENERAL_RESPONSE,
    KeyParam = {MsgId, MsgSn},
    Inflight2 = ack_msg(AckMsgId, KeyParam, Inflight1, Channel),

    %% Verify auto key was removed
    AutoKey = set_msg_ack(MsgId, MsgSn),
    ?assertNot(emqx_inflight:contain(AutoKey, Inflight2)).

%% Test: ACK when only custom msg exists
t_ack_only_custom(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert custom msg_sn
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight0, Channel
    ),

    %% ACK the message
    AckMsgId = ?MC_GENERAL_RESPONSE,
    KeyParam = {MsgId, MsgSn},
    Inflight2 = ack_msg(AckMsgId, KeyParam, Inflight1, Channel),

    %% Verify custom key was removed
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    ?assertNot(emqx_inflight:contain(CustomKey, Inflight2)).

%% Test: ACK when both exist and auto was first - should remove auto
t_ack_race_auto_first(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert auto first, then custom
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight0, Channel
    ),
    {ok, Inflight2} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight1, Channel
    ),

    AutoKey = set_msg_ack(MsgId, MsgSn),
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),

    %% Verify both exist before ACK
    ?assert(emqx_inflight:contain(AutoKey, Inflight2)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight2)),

    %% First ACK should remove auto (it was first)
    AckMsgId = ?MC_GENERAL_RESPONSE,
    KeyParam = {MsgId, MsgSn},
    Inflight3 = ack_msg(AckMsgId, KeyParam, Inflight2, Channel),

    ?assertNot(emqx_inflight:contain(AutoKey, Inflight3)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight3)),

    %% Second ACK should remove custom
    Inflight4 = ack_msg(AckMsgId, KeyParam, Inflight3, Channel),

    ?assertNot(emqx_inflight:contain(AutoKey, Inflight4)),
    ?assertNot(emqx_inflight:contain(CustomKey, Inflight4)).

%% Test: ACK when both exist and custom was first - should remove custom
t_ack_race_custom_first(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert custom first
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight0, Channel
    ),

    %% Then insert auto (simulating channel counter catching up)
    {ok, Inflight2} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight1, Channel
    ),

    AutoKey = set_msg_ack(MsgId, MsgSn),
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),

    %% Verify both exist before ACK
    ?assert(emqx_inflight:contain(AutoKey, Inflight2)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight2)),

    %% First ACK should remove custom (it was first)
    AckMsgId = ?MC_GENERAL_RESPONSE,
    KeyParam = {MsgId, MsgSn},
    Inflight3 = ack_msg(AckMsgId, KeyParam, Inflight2, Channel),

    ?assert(emqx_inflight:contain(AutoKey, Inflight3)),
    ?assertNot(emqx_inflight:contain(CustomKey, Inflight3)),

    %% Second ACK should remove auto
    Inflight4 = ack_msg(AckMsgId, KeyParam, Inflight3, Channel),

    ?assertNot(emqx_inflight:contain(AutoKey, Inflight4)),
    ?assertNot(emqx_inflight:contain(CustomKey, Inflight4)).

%% Test: ACK when neither exists (edge case)
%% This can happen when duplicate messages are delivered and client sends multiple ACKs
t_ack_neither_exists(_Config) ->
    Inflight = emqx_inflight:new(128),
    MsgId = ?MS_SEND_TEXT,
    MsgSn = 100,
    Channel = make_test_channel(),

    %% ACK a non-existent message should just return the inflight unchanged
    %% (and log a warning internally)
    AckMsgId = ?MC_GENERAL_RESPONSE,
    KeyParam = {MsgId, MsgSn},
    ResultInflight = ack_msg(AckMsgId, KeyParam, Inflight, Channel),

    ?assertEqual(emqx_inflight:size(Inflight), emqx_inflight:size(ResultInflight)).

%%--------------------------------------------------------------------
%% Test Cases for different message types
%%--------------------------------------------------------------------

%% Test: Query param message type (different ACK key format)
t_query_param_msg_type(_Config) ->
    Inflight0 = emqx_inflight:new(128),
    MsgId = ?MS_QUERY_CLIENT_PARAM,
    MsgSn = 200,
    Frame = make_frame(MsgId, MsgSn),
    RetxMax = 5,
    Channel = make_test_channel(),

    %% Insert auto
    {ok, Inflight1} = try_insert_inflight(
        MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight0, Channel
    ),

    %% Insert custom
    {ok, Inflight2} = try_insert_inflight(
        MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight1, Channel
    ),

    %% Verify keys format
    AutoKey = set_msg_ack(MsgId, MsgSn),
    ?assertEqual({?MC_QUERY_PARAM_ACK, MsgSn}, AutoKey),

    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    ?assertEqual({custom, ?MC_QUERY_PARAM_ACK, MsgSn}, CustomKey),

    ?assert(emqx_inflight:contain(AutoKey, Inflight2)),
    ?assert(emqx_inflight:contain(CustomKey, Inflight2)).

%%--------------------------------------------------------------------
%% Test Cases for normalize_queue_item (hot upgrade compatibility)
%%--------------------------------------------------------------------

%% Test: New format {Frame, SnType} should be returned as-is
t_normalize_queue_item_new_format_auto(_Config) ->
    Frame = make_frame(?MS_SEND_TEXT, 100),
    Result = normalize_queue_item({Frame, ?SN_AUTO}),
    ?assertEqual({Frame, ?SN_AUTO}, Result).

t_normalize_queue_item_new_format_custom(_Config) ->
    Frame = make_frame(?MS_SEND_TEXT, 100),
    Result = normalize_queue_item({Frame, ?SN_CUSTOM}),
    ?assertEqual({Frame, ?SN_CUSTOM}, Result).

%% Test: Old format (just Frame map) should be treated as auto for hot upgrade
t_normalize_queue_item_old_format(_Config) ->
    Frame = make_frame(?MS_SEND_TEXT, 100),
    Result = normalize_queue_item(Frame),
    ?assertEqual({Frame, ?SN_AUTO}, Result).

%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

make_frame(MsgId, MsgSn) ->
    #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"msg_sn">> => MsgSn,
            <<"phone">> => <<"000123456789">>
        },
        <<"body">> => #{}
    }.
