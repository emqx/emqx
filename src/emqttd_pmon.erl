%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_pmon).

-author("Feng Lee <feng@emqtt.io>").

-export([new/0, monitor/2, demonitor/2, erase/2]).

-type(pmon() :: {?MODULE, map()}).

-export_type([pmon/0]).

new() ->
    {?MODULE, [maps:new()]}.

-spec(monitor(pid(), pmon()) -> pmon()).
monitor(Pid, PM = {?MODULE, [M]}) ->
    case maps:is_key(Pid, M) of
        true ->
            PM;
        false ->
            Ref = erlang:monitor(process, Pid),
            {?MODULE, [maps:put(Pid, Ref, M)]}
    end.

-spec(demonitor(pid(), pmon()) -> pmon()).
demonitor(Pid, PM = {?MODULE, [M]}) ->
    case maps:find(Pid, M) of
        {ok, Ref} ->
            erlang:demonitor(Ref, [flush]),
            {?MODULE, [maps:remove(Pid, M)]};
        error ->
            PM
    end.

-spec(erase(pid(), pmon()) -> pmon()).
erase(Pid, {?MODULE, [M]}) ->
    {?MODULE, [maps:remove(Pid, M)]}.

