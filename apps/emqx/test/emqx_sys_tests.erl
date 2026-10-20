%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_tests).

-include_lib("eunit/include/eunit.hrl").

escape_clientid_unchanged_test() ->
    ClientId = binary:copy(<<"ordinary/client:id">>, 8),
    ?assert(erts_debug:same(ClientId, emqx_sys:escape_clientid(ClientId))).

event_topic_clientid_test_() ->
    [
        {ClientId, fun() ->
            Topic = emqx_sys:event_topic(connected, #{
                clientid => ClientId,
                protocol => Protocol
            }),
            ?assertEqual(emqx_topic:systop(Expected), Topic),
            ?assert(emqx_topic:validate(name, Topic))
        end}
     || {Protocol, ClientId, Expected} <- [
            {mqtt, <<"sutekasdf/#">>, "clients/sutekasdf/%23/connected"},
            {stomp, <<"a/+/b">>, "gateway/stomp/clients/a/%2B/b/connected"},
            {mqtt, <<"a+#b++#">>, "clients/a%2B%23b%2B%2B%23/connected"},
            {mqtt, <<"%23/%2B/%">>, "clients/%2523/%252B/%25/connected"},
            {mqtt, <<"ordinary/client:id">>, "clients/ordinary/client:id/connected"}
        ]
    ].
