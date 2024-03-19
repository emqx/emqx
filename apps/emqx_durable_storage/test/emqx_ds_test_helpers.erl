%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

%% RPC mocking

mock_rpc() ->
    ok = meck:new(erpc, [passthrough, no_history, unstick]),
    ok = meck:new(gen_rpc, [passthrough, no_history]).

unmock_rpc() ->
    catch meck:unload(erpc),
    catch meck:unload(gen_rpc).

mock_rpc_result(ExpectFun) ->
    mock_rpc_result(erpc, ExpectFun),
    mock_rpc_result(gen_rpc, ExpectFun).

mock_rpc_result(erpc, ExpectFun) ->
    ok = meck:expect(erpc, call, fun(Node, Mod, Function, Args) ->
        case ExpectFun(Node, Mod, Function, Args) of
            passthrough ->
                meck:passthrough([Node, Mod, Function, Args]);
            unavailable ->
                meck:exception(error, {erpc, noconnection});
            {timeout, Timeout} ->
                ok = timer:sleep(Timeout),
                meck:exception(error, {erpc, timeout})
        end
    end);
mock_rpc_result(gen_rpc, ExpectFun) ->
    ok = meck:expect(gen_rpc, call, fun(Dest = {Node, _}, Mod, Function, Args) ->
        case ExpectFun(Node, Mod, Function, Args) of
            passthrough ->
                meck:passthrough([Dest, Mod, Function, Args]);
            unavailable ->
                {badtcp, econnrefused};
            {timeout, Timeout} ->
                ok = timer:sleep(Timeout),
                {badrpc, timeout}
        end
    end).
