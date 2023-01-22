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
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([
    create_tab/0,
    hook/0,
    unhook/0
]).

-export([
    on_channel_unregistered/1,
    on_channel_takeover/3,
    on_channel_takenover/3,
    on_message_publish/1,
    on_message_puback/4
]).

%% For Debug
-export([transfer/2, storage/0]).

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
    ok = emqx_hooks:put('channel.unregistered', {?MODULE, on_channel_unregistered, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('channel.takeover', {?MODULE, on_channel_takeover, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('channel.takenover', {?MODULE, on_channel_takenover, []}, ?HP_LOWEST),

    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('message.puback', {?MODULE, on_message_puback, []}, ?HP_LOWEST).

unhook() ->
    ok = emqx_hooks:del('channel.unregistered', {?MODULE, on_channel_unregistered}),
    ok = emqx_hooks:del('channel.takeover', {?MODULE, on_channel_takeover}),
    ok = emqx_hooks:del('channel.takenover', {?MODULE, on_channel_takenover}),

    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_hooks:del('message.puback', {?MODULE, on_message_puback}).

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

on_channel_takenover(_ConnMod, ChanPid, #{ft_data := FTData}) ->
    ok = put_ft_data(ChanPid, FTData);
on_channel_takenover(_ConnMod, _ChanPid, _) ->
    ok.

on_message_publish(
    Msg = #message{
        id = _Id,
        topic = <<"$file/", _/binary>>
    }
) ->
    Headers = Msg#message.headers,
    {stop, Msg#message{headers = Headers#{allow_publish => false}}};
on_message_publish(Msg) ->
    {ok, Msg}.

on_message_puback(PacketId, #message{topic = Topic} = Msg, _PubRes, _RC) ->
    case Topic of
        <<"$file/", FileCommand/binary>> ->
            {stop, on_file_command(PacketId, Msg, FileCommand)};
        _ ->
            ignore
    end.

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

on_file_command(PacketId, Msg, FileCommand) ->
    case string:split(FileCommand, <<"/">>, all) of
        [FileId, <<"init">>] ->
            on_init(Msg, FileId);
        [FileId, <<"fin">>] ->
            on_fin(PacketId, Msg, FileId, undefined);
        [FileId, <<"fin">>, Checksum] ->
            on_fin(PacketId, Msg, FileId, Checksum);
        [FileId, <<"abort">>] ->
            on_abort(Msg, FileId);
        [FileId, Offset] ->
            on_segment(Msg, FileId, Offset, undefined);
        [FileId, Offset, Checksum] ->
            on_segment(Msg, FileId, Offset, Checksum);
        _ ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_init(Msg, FileId) ->
    ?SLOG(info, #{
        msg => "on_init",
        mqtt_msg => Msg,
        file_id => FileId
    }),
    % Payload = Msg#message.payload,
    % %% Add validations here
    % Meta = emqx_json:decode(Payload, [return_maps]),
    % ok = emqx_ft_storage_fs:store_filemeta(storage(), transfer(Msg, FileId), Meta),
    % ?RC_SUCCESS.
    ?RC_UNSPECIFIED_ERROR.

on_abort(_Msg, _FileId) ->
    %% TODO
    ?RC_SUCCESS.

on_segment(Msg, FileId, Offset, Checksum) ->
    ?SLOG(info, #{
        msg => "on_segment",
        mqtt_msg => Msg,
        file_id => FileId,
        offset => Offset,
        checksum => Checksum
    }),
    % %% TODO: handle checksum
    % Payload = Msg#message.payload,
    % %% Add offset/checksum validations
    % ok = emqx_ft_storage_fs:store_segment(
    %     storage(),
    %     transfer(Msg, FileId),
    %     {binary_to_integer(Offset), Payload}
    % ),
    % ?RC_SUCCESS.
    ?RC_UNSPECIFIED_ERROR.

on_fin(PacketId, Msg, FileId, Checksum) ->
    ?SLOG(info, #{
        msg => "on_fin",
        mqtt_msg => Msg,
        file_id => FileId,
        checksum => Checksum,
        packet_id => PacketId
    }),
    % %% TODO: handle checksum? Do we need it?
    % {ok, _} = emqx_ft_storage_fs:assemble(
    %     storage(),
    %     transfer(Msg, FileId),
    %     callback(FileId, Msg)
    % ),
    Callback = callback(FileId, PacketId),
    spawn(fun() -> Callback({error, not_implemented}) end),
    undefined.

callback(_FileId, PacketId) ->
    ChanPid = self(),
    fun
        (ok) ->
            erlang:send(ChanPid, {puback, PacketId, [], ?RC_SUCCESS});
        ({error, _}) ->
            erlang:send(ChanPid, {puback, PacketId, [], ?RC_UNSPECIFIED_ERROR})
    end.

transfer(Msg, FileId) ->
    ClientId = Msg#message.from,
    {ClientId, FileId}.

%% TODO: configure
storage() ->
    filename:join(emqx:data_dir(), "file_transfer").
