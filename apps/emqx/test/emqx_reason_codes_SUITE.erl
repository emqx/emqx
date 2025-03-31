%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_reason_codes_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_frame_error(_) ->
    ?assertEqual(?RC_PACKET_TOO_LARGE, emqx_reason_codes:frame_error(frame_too_large)),
    ?assertEqual(?RC_MALFORMED_PACKET, emqx_reason_codes:frame_error(bad_packet_id)),
    ?assertEqual(?RC_MALFORMED_PACKET, emqx_reason_codes:frame_error(bad_qos)).
