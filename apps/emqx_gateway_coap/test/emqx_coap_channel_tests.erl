%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_channel_tests).

-include_lib("eunit/include/eunit.hrl").

sock_closed_cleanup_uses_keepalive_interval_test() ->
    ok = meck:new(emqx_utils, [passthrough]),
    ok = meck:new(emqx_keepalive, [passthrough]),
    try
        meck:expect(emqx_utils, start_timer, fun(Time, Msg) ->
            %% Do not schedule a real timer here: leaked timeout messages can pollute
            %% subsequent EUnit tests that reuse the same worker process.
            {mock_timer_ref, Time, Msg}
        end),
        Keepalive = keepalive_ref,
        Expected = 30000,
        meck:expect(emqx_keepalive, info, fun(check_interval, keepalive_ref) -> Expected end),
        Channel = {channel, #{}, #{}, #{}, undefined, Keepalive, #{}, true, connected, undefined},
        {ok, _Channel2} = emqx_coap_channel:handle_info({sock_closed, closed}, Channel),
        ?assert(meck:called(emqx_utils, start_timer, [Expected, sock_closed_takeover_cleanup]))
    after
        meck:unload(emqx_keepalive),
        meck:unload(emqx_utils)
    end.
