%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_keepalive_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> [{group, keepalive}].

groups() -> [{keepalive, [], [t_keepalive]}].

%%--------------------------------------------------------------------
%% Keepalive
%%--------------------------------------------------------------------

t_keepalive(_) ->
    {ok, KA} = emqx_keepalive:start(fun() -> {ok, 1} end, 1, {keepalive, timeout}),
    [resumed, timeout] = lists:reverse(keepalive_recv(KA, [])).

keepalive_recv(KA, Acc) ->
    receive
        {keepalive, timeout} ->
            case emqx_keepalive:check(KA) of
                {ok, KA1} -> keepalive_recv(KA1, [resumed | Acc]);
                {error, timeout} -> [timeout | Acc]
            end
        after 4000 ->
                Acc
    end.

