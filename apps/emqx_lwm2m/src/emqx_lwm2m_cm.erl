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

-module(emqx_lwm2m_cm).

-export([start_link/0]).

-export([ register_channel/5
        , update_reg_info/2
        , unregister_channel/1
        ]).

-export([ lookup_channel/1
        , all_channels/0
        ]).

-export([ register_cmd/3
        , register_cmd/4
        , lookup_cmd/3
        , lookup_cmd_by_imei/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(LOG(Level, Format, Args), logger:Level("LWM2M-CM: " ++ Format, Args)).

%% Server name
-define(CM, ?MODULE).

-define(LWM2M_CHANNEL_TAB, emqx_lwm2m_channel).
-define(LWM2M_CMD_TAB, emqx_lwm2m_cmd).

%% Batch drain
-define(BATCH_SIZE, 100000).

%% @doc Start the channel manager.
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

register_channel(IMEI, RegInfo, LifeTime, Ver, Peername) ->
    Info = #{
        reg_info => RegInfo,
        lifetime => LifeTime,
        version => Ver,
        peername => Peername
    },
    true = ets:insert(?LWM2M_CHANNEL_TAB, {IMEI, Info}),
    cast({registered, {IMEI, self()}}).

update_reg_info(IMEI, RegInfo) ->
    case lookup_channel(IMEI) of
        [{_, RegInfo0}] ->
            true = ets:insert(?LWM2M_CHANNEL_TAB, {IMEI, RegInfo0#{reg_info => RegInfo}}),
            ok;
        [] ->
            ok
    end.

unregister_channel(IMEI) when is_binary(IMEI) ->
    true = ets:delete(?LWM2M_CHANNEL_TAB, IMEI),
    ok.

lookup_channel(IMEI) ->
    ets:lookup(?LWM2M_CHANNEL_TAB, IMEI).

all_channels() ->
    ets:tab2list(?LWM2M_CHANNEL_TAB).

register_cmd(IMEI, Path, Type) ->
    true = ets:insert(?LWM2M_CMD_TAB, {{IMEI, Path, Type}, undefined}).

register_cmd(_IMEI, undefined, _Type, _Result) ->
    ok;
register_cmd(IMEI, Path, Type, Result) ->
    true = ets:insert(?LWM2M_CMD_TAB, {{IMEI, Path, Type}, Result}).

lookup_cmd(IMEI, Path, Type) ->
    ets:lookup(?LWM2M_CMD_TAB, {IMEI, Path, Type}).

lookup_cmd_by_imei(IMEI) ->
    ets:select(?LWM2M_CHANNEL_TAB, [{{{IMEI, '_', '_'}, '$1'}, [], ['$_']}]).

%% @private
cast(Msg) -> gen_server:cast(?CM, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    TabOpts = [public, {write_concurrency, true}, {read_concurrency, true}],
    ok = emqx_tables:new(?LWM2M_CHANNEL_TAB, [set, compressed | TabOpts]),
    ok = emqx_tables:new(?LWM2M_CMD_TAB, [set, compressed | TabOpts]),
    {ok, #{chan_pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({registered, {IMEI, ChanPid}}, State = #{chan_pmon := PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, IMEI, PMon),
    {noreply, State#{chan_pmon := PMon1}};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{chan_pmon := PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),
    ok = emqx_pool:async_submit(fun lists:foreach/2, [fun clean_down/1, Items]),
    {noreply, State#{chan_pmon := PMon1}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(chan_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

clean_down({_ChanPid, IMEI}) ->
    unregister_channel(IMEI).
