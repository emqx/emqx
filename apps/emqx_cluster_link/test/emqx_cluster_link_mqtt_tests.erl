%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

%% A message forwarded to a remote cluster does not carry the local
%% `message_persisted' header.
forward_drops_persisted_header_test() ->
    Msg = emqx_message:make(<<"t/link">>, <<"payload">>),
    Persisted = emqx_message:set_header(message_persisted, true, Msg),
    ok = meck:new(emqx_resource, [no_link]),
    try
        ok = meck:expect(emqx_resource, query, fun(_ResId, Query, _QueryOpts) -> Query end),
        FwdMsg = emqx_cluster_link_mqtt:forward(
            <<"remote">>, #delivery{sender = self(), message = Persisted}
        ),
        ?assertEqual(Msg#message.headers, FwdMsg#message.headers)
    after
        ok = meck:unload(emqx_resource)
    end.
