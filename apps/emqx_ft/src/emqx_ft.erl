%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft).

-include("emqx_ft.hrl").

-export([
    create_tab/0,
    hook/0,
    unhook/0
]).

-export([
    on_channel_unregistered/1,
    on_channel_takeover/3,
    on_channel_takeovered/3
]).

-export_type([clientid/0]).
-export_type([transfer/0]).
-export_type([offset/0]).

%% Number of bytes
-type bytes() :: non_neg_integer().

%% MQTT Client ID
-type clientid() :: emqx_types:clientid().

-type fileid() :: binary().
-type transfer() :: {clientid(), fileid()}.
-type offset() :: bytes().

-type ft_data() :: #{
    nodes := list(node())
}.

-record(emqx_ft, {
    chan_pid :: pid(),
    ft_data :: ft_data()
}).

%%--------------------------------------------------------------------
%% API for app
%%--------------------------------------------------------------------

create_tab() ->
    _Tab = ets:new(?FT_TAB, [
        set,
        public,
        named_table,
        {keypos, #emqx_ft.chan_pid}
    ]),
    ok.

hook() ->
    % ok = emqx_hooks:put('channel.registered', {?MODULE, on_channel_registered, []}),
    ok = emqx_hooks:put('channel.unregistered', {?MODULE, on_channel_unregistered, []}),
    ok = emqx_hooks:put('channel.takeover', {?MODULE, on_channel_takeover, []}),
    ok = emqx_hooks:put('channel.takeovered', {?MODULE, on_channel_takeovered, []}).

unhook() ->
    % ok = emqx_hooks:del('channel.registered', {?MODULE, on_channel_registered}),
    ok = emqx_hooks:del('channel.unregistered', {?MODULE, on_channel_unregistered}),
    ok = emqx_hooks:del('channel.takeover', {?MODULE, on_channel_takeover}),
    ok = emqx_hooks:del('channel.takeovered', {?MODULE, on_channel_takeovered}).

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_channel_unregistered(ChanPid) ->
    ok = delete_ft_data(ChanPid).

on_channel_takeover(_ConnMod, ChanPid, TakeoverData) ->
    case get_ft_data(ChanPid) of
        {ok, FTData} ->
            {ok, TakeoverData#{ft_data => FTData}};
        none ->
            ok
    end.

on_channel_takeovered(_ConnMod, ChanPid, #{ft_data := FTData}) ->
    ok = put_ft_data(ChanPid, FTData);
on_channel_takeovered(_ConnMod, _ChanPid, _) ->
    ok.

%%--------------------------------------------------------------------
%% Private funs
%%--------------------------------------------------------------------

get_ft_data(ChanPid) ->
    case ets:lookup(?FT_TAB, ChanPid) of
        [#emqx_ft{ft_data = FTData}] -> {ok, FTData};
        [] -> none
    end.

delete_ft_data(ChanPid) ->
    true = ets:delete(?FT_TAB, ChanPid),
    ok.

put_ft_data(ChanPid, FTData) ->
    true = ets:insert(?FT_TAB, #emqx_ft{chan_pid = ChanPid, ft_data = FTData}),
    ok.
