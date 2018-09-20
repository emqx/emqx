%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    {ok, KA} = emqx_keepalive:start(1, {keepalive, timeout}),
    resumed = keepalive_recv(KA, 100),
    timeout = keepalive_recv(KA, 2000).

keepalive_recv(KA, MockInterval) ->
    receive
        {keepalive, timeout} ->
            case emqx_keepalive:check(KA, erlang:system_time(millisecond) - MockInterval) of
                {ok, _} -> resumed;
                {error, timeout} -> timeout
            end
        after 4000 ->
                error
    end.

