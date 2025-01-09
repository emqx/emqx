%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_resource_cache).

%% CRUD APIs
-export([new/0, write/3, is_exist/1, read/1, erase/1]).
%% For Config management
-export([all_ids/0, list_all/0, group_ids/1]).
%% For health checks etc.
-export([read_status/1, read_mod/1, read_manager_pid/1]).
%% Hot-path
-export([get_runtime/1]).

-include("emqx_resource_runtime.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(CACHE, ?RESOURCE_CACHE).
-define(NO_CB, no_state).

-record(connector, {
    id :: binary(),
    group :: binary(),
    manager_pid :: pid(),
    st_err :: st_err(),
    config :: term(),
    cb = ?NO_CB :: term(),
    extra = []
}).

-type chan_key() :: {connector_resource_id(), channel_id()}.

-record(channel, {
    id :: chan_key(),
    error :: term(),
    status :: channel_status(),
    query_mode :: emqx_resource:resource_query_mode(),
    extra = []
}).

-define(CB_PT_KEY(ID), {?MODULE, ID}).

new() ->
    emqx_utils_ets:new(?CACHE, [
        ordered_set,
        public,
        {read_concurrency, true},
        {keypos, 2}
    ]).

-spec write(pid(), binary(), resource_data()) -> ok.
write(ManagerPid, Group, Data) ->
    #{
        id := ID,
        mod := Mod,
        callback_mode := CallbackMode,
        query_mode := QueryMode,
        config := Config,
        error := Error,
        state := State,
        status := Status,
        added_channels := AddedChannels
    } = Data,
    Cb = #{
        mod => Mod,
        callback_mode => CallbackMode,
        query_mode => QueryMode,
        state => State
    },
    IsDryrun = emqx_resource:is_dry_run(ID),
    Connector = #connector{
        id = ID,
        group = Group,
        manager_pid = ManagerPid,
        st_err = #{
            status => Status,
            error => external_error(Error)
        },
        config = Config,
        cb =
            case IsDryrun of
                true ->
                    %% save callback state in ets for dryrun
                    Cb;
                false ->
                    ?NO_CB
            end,
        extra = []
    },
    Channels = lists:map(fun to_channel_record/1, maps:to_list(AddedChannels)),
    %% erase old channels (if any)
    ok = erase_old_channels(ID, maps:keys(AddedChannels)),
    %% put callback state in persistent_term
    case IsDryrun of
        true ->
            %% do not write persistent_term for dryrun
            ok;
        false ->
            ok = put_state_pt(ID, Cb)
    end,
    %% insert connector and channel states
    true = ets:insert(?CACHE, [Connector | Channels]),
    ok.

%% @doc Read cached pieces and return a externalized map.
%% NOTE: Do not call this in hot-path.
%% TODO: move `group' into `resource_data()'.
-spec read(resource_id()) -> [{resource_group(), resource_data()}].
read(ID) ->
    case ets:lookup(?CACHE, ID) of
        [] ->
            [];
        [#connector{group = G} = C] ->
            Channels = find_channels(ID),
            [{G, make_resource_data(ID, C, Channels)}]
    end.

-spec read_status(resource_id()) -> not_found | st_err().
read_status(ID) ->
    ets:lookup_element(?CACHE, ID, #connector.st_err, not_found).

-spec read_manager_pid(resource_id()) -> not_found | pid().
read_manager_pid(ID) ->
    ets:lookup_element(?CACHE, ID, #connector.manager_pid, not_found).

-spec read_mod(resource_id()) -> not_found | {ok, module()}.
read_mod(ID) ->
    case get_cb(ID) of
        ?NO_CB ->
            not_found;
        #{mod := Mod} ->
            {ok, Mod}
    end.

get_cb(ID) ->
    case get_cb_pt(ID) of
        ?NO_CB ->
            %% maybe it's a dryrun connector
            ets:lookup_element(?CACHE, ID, #connector.cb, ?NO_CB);
        InPt ->
            InPt
    end.

-spec erase(resource_id()) -> ok.
erase(ID) ->
    MS = ets:fun2ms(fun(#channel{id = {C, _}}) when C =:= ID -> true end),
    _ = ets:select_delete(?CACHE, MS),
    _ = ets:delete(?CACHE, ID),
    _ = del_state_pt(?CB_PT_KEY(ID)),
    ok.

erase_old_channels(ID, NewChanIds) ->
    OldChanIds = maps:keys(find_channels(ID)),
    DelChanIds = OldChanIds -- NewChanIds,
    lists:foreach(fun erase_channel/1, DelChanIds).

erase_channel(ChanId) ->
    Key = split_channel_id(ChanId),
    ets:delete(?CACHE, Key).

-spec list_all() -> [resource_data()].
list_all() ->
    IDs = all_ids(),
    lists:foldr(
        fun(ID, Acc) ->
            case read(ID) of
                [] ->
                    Acc;
                [{_G, Data}] ->
                    [Data | Acc]
            end
        end,
        [],
        IDs
    ).

group_ids(Group) ->
    MS = ets:fun2ms(fun(#connector{id = ID, group = G}) when G =:= Group -> ID end),
    ets:select(?CACHE, MS).

all_ids() ->
    MS = ets:fun2ms(fun(#connector{id = ID}) -> ID end),
    ets:select(?CACHE, MS).

%% @doc The most performance-critical call.
%% NOTE: ID is the action ID, but not connector ID.
-spec get_runtime(resource_id()) -> {ok, runtime()} | {error, not_found}.
get_runtime(ID) ->
    ChanKey = {ConnectorId, _ChanID} = split_channel_id(ID),
    try
        Cb = get_cb(ConnectorId),
        ChannelStatus = get_channel_status(ChanKey),
        ChannelQueryMode = get_channel_query_mode(ChanKey),
        StErr = ets:lookup_element(?CACHE, ConnectorId, #connector.st_err),
        {ok, #rt{
            st_err = StErr,
            cb = Cb,
            query_mode = ChannelQueryMode,
            channel_status = ChannelStatus
        }}
    catch
        error:badarg ->
            {error, not_found}
    end.

get_channel_status({_, ?NO_CHANNEL}) ->
    ?NO_CHANNEL;
get_channel_status(ChanKey) ->
    ets:lookup_element(?CACHE, ChanKey, #channel.status, ?NO_CHANNEL).

get_channel_query_mode({_, ?NO_CHANNEL}) ->
    ?NO_CHANNEL;
get_channel_query_mode(ChanKey) ->
    ets:lookup_element(?CACHE, ChanKey, #channel.query_mode, ?NO_CHANNEL).

get_cb_pt(ID) ->
    persistent_term:get(?CB_PT_KEY(ID), ?NO_CB).

to_channel_record({ID0, #{status := Status, error := Error, query_mode := QueryMode}}) ->
    ID = split_channel_id(ID0),
    #channel{
        id = ID,
        status = Status,
        error = Error,
        query_mode = QueryMode,
        extra = []
    }.

split_channel_id(Id) when is_binary(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [
            ChannelGlobalType,
            ChannelSubType,
            ChannelName,
            <<"connector">>,
            ConnectorType,
            ConnectorName
        ] ->
            ConnectorId = <<"connector:", ConnectorType/binary, ":", ConnectorName/binary>>,
            ChannelId =
                <<ChannelGlobalType/binary, ":", ChannelSubType/binary, ":", ChannelName/binary>>,
            {ConnectorId, ChannelId};
        _ ->
            %% this is not a per-channel query, e.g. for authn/authz
            {Id, ?NO_CHANNEL}
    end.

%% State can be quite bloated, caching it in ets means excessive large term copies,
%% for each and every query so we keep it in persistent_term instead.
%% Connector state is relatively static, so persistent_term update triggered GC is less of a concern
%% comparing to other fields such as `status' and `error', which may change very often.
put_state_pt(ID, State) ->
    case get_cb_pt(ID) of
        S when S =:= State ->
            %% identical
            ok;
        _ ->
            _ = persistent_term:put(?CB_PT_KEY(ID), State),
            ok
    end.

del_state_pt(ID) ->
    _ = persistent_term:erase(?CB_PT_KEY(ID)),
    ok.

is_exist(ID) ->
    ets:member(?CACHE, ID).

make_resource_data(ID, Connector, Channels) ->
    #connector{
        st_err = #{
            error := Error,
            status := Status
        },
        config = Config,
        cb = Cb0
    } = Connector,
    Cb =
        case Cb0 of
            ?NO_CB ->
                get_cb_pt(ID);
            X ->
                X
        end,
    #{
        mod := Mod,
        callback_mode := CallbackMode,
        query_mode := QueryMode,
        state := State
    } = Cb,
    #{
        id => ID,
        mod => Mod,
        callback_mode => CallbackMode,
        query_mode => QueryMode,
        error => Error,
        status => Status,
        config => Config,
        added_channels => Channels,
        state => State
    }.

find_channels(ConnectorId) ->
    MS = ets:fun2ms(fun(#channel{id = {Cid, _}} = C) when Cid =:= ConnectorId -> C end),
    List = ets:select(?CACHE, MS),
    lists:foldl(
        fun(
            #channel{
                id = {ConnectorId0, ChannelId},
                status = Status,
                query_mode = QueryMode,
                error = Error
            },
            Acc
        ) ->
            Key = iolist_to_binary([ChannelId, ":", ConnectorId0]),
            Acc#{Key => #{status => Status, error => Error, query_mode => QueryMode}}
        end,
        #{},
        List
    ).

external_error({error, Reason}) -> Reason;
external_error(Other) -> Other.
