%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_observe_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_observe_manager(_) ->
    Manager0 = emqx_coap_observe_res:new_manager(),
    Sub = #{topic => <<"t">>, token => <<"tok">>, subopts => #{qos => 0}},
    {SeqId, Manager1} = emqx_coap_observe_res:insert(Sub, Manager0),
    {SeqId2, Manager2} = emqx_coap_observe_res:insert(Sub, Manager1),
    ?assertEqual(SeqId, SeqId2),
    _ = emqx_coap_observe_res:remove(<<"t">>, Manager2),
    Manager3 = #{
        <<"wrap">> => #{token => <<"t">>, seq_id => 16777215, subopts => #{qos => 0}}
    },
    {_, SeqIdWrap, _} = emqx_coap_observe_res:res_changed(<<"wrap">>, Manager3),
    ?assert(SeqIdWrap >= 16777216),
    SeqVal = emqx_coap_observe_res:current_value(),
    ?assert(SeqVal >= 0 andalso SeqVal =< 16#FFFFFF),
    ?assertEqual(SeqIdWrap band 16#FFFFFF, emqx_coap_observe_res:observe_value(SeqIdWrap)),
    ok = emqx_coap_observe_res:foreach(fun(_, _) -> ok end, Manager2),
    _ = emqx_coap_observe_res:subscriptions(Manager2),
    ok.
