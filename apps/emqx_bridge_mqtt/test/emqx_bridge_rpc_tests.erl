%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_rpc_tests).
-include_lib("eunit/include/eunit.hrl").

send_and_ack_test() ->
    %% delegate from emqx_rpc to rpc for unit test
    meck:new(emqx_rpc, [passthrough, no_history]),
    meck:expect(emqx_rpc, call, 4,
                fun(Node, Module, Fun, Args) ->
                        rpc:call(Node, Module, Fun, Args)
                end),
    meck:expect(emqx_rpc, cast, 4,
                fun(Node, Module, Fun, Args) ->
                        rpc:cast(Node, Module, Fun, Args)
                end),
    meck:new(emqx_bridge_worker, [passthrough, no_history]),
    try
        {ok, #{client_pid := Pid, address := Node}} = emqx_bridge_rpc:start(#{address => node()}),
        {ok, Ref} = emqx_bridge_rpc:send(#{address => Node}, []),
        receive
            {batch_ack, Ref} ->
                ok
        end,
        ok = emqx_bridge_rpc:stop( #{client_pid => Pid})
    after
        meck:unload(emqx_rpc)
    end.
