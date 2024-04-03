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

-module(emqx_ds_replication_snapshot).

-include_lib("snabbkaffe/include/trace.hrl").

-behaviour(ra_snapshot).
-export([
    prepare/2,
    write/3,

    begin_read/2,
    read_chunk/3,

    begin_accept/2,
    accept_chunk/2,
    complete_accept/2,

    recover/1,
    validate/1,
    read_meta/1
]).

-type rs() :: emqx_ds_snapshot_manager:ext_rs().
-type ws() :: emqx_ds_snapshot_manager:ext_ws().

-type ra_state() :: emqx_ds_replication_layer:ra_state().

%% Writing a snapshot.
%% This process is exactly the same as writing a ra log snapshot: store the
%% log meta and the machine state in a single snapshot file.

-spec prepare(_RaftIndex, ra_state()) -> _State :: ra_state().
prepare(Index, State) ->
    ra_log_snapshot:prepare(Index, State).

-spec write(_SnapshotDir :: file:filename(), ra_snapshot:meta(), _State :: ra_state()) ->
    ok | {ok, _BytesWritten :: non_neg_integer()} | {error, ra_snapshot:file_err()}.
write(Dir, Meta, MachineState) ->
    ra_log_snapshot:write(Dir, Meta, MachineState).

%% Reading a snapshot.
%%
%% This is triggered by the leader when it finds out that a follower is
%% behind so much that there are no log segments covering the gap anymore.
%% This process, on the other hand, MUST involve reading the storage snapshot,
%% (in addition to the log snapshot) to reconstruct the storage state on the
%% target server.
%%
%% Currently, a snapshot reader is owned by a special "snapshot sender" process
%% spawned by the leader `ra` server, which sends chunks to the target server
%% in a tight loop. This process terminates under the following conditions:
%% 1. The snapshot is completely read and sent.
%% 2. Remote server fails to accept a chunk, either due to network failure (most
%%    likely) or a logic error (very unlikely).

-spec begin_read(_SnapshotDir :: file:filename(), _Context :: #{}) ->
    {ok, ra_snapshot:meta(), rs()} | {error, _Reason :: term()}.
begin_read(Dir, Context) ->
    emqx_ds_snapshot_manager:begin_read(Dir, Context).

-spec read_chunk(rs(), _Size :: non_neg_integer(), _SnapshotDir :: file:filename()) ->
    {ok, binary(), {next, rs()} | last} | {error, _Reason :: term()}.
read_chunk(RS, Size, Dir) ->
    emqx_ds_snapshot_manager:read_chunk(RS, Size, Dir).

%% Accepting a snapshot.
%%
%% This process is triggered by the target server, when the leader finds out
%% that the target server is severely lagging behind. This is receiving side of
%% `begin_read/2` and `read_chunk/3`.
%%
%% Currently, a snapshot writer is owned by the follower `ra` server process
%% residing in dedicated `receive_snapshot` state. This process reverts back
%% to the regular `follower` state under the following conditions:
%% 1. The snapshot is completely accepted, and the machine state is recovered.
%% 2. The process times out waiting for the next chunk.
%% 3. The process encounters a logic error (very unlikely).

-spec begin_accept(_SnapshotDir :: file:filename(), ra_snapshot:meta()) ->
    {ok, ws()}.
begin_accept(Dir, Meta) ->
    emqx_ds_snapshot_manager:begin_accept(Dir, Meta).

-spec accept_chunk(binary(), ws()) ->
    {ok, ws()} | {error, _Reason :: term()}.
accept_chunk(Chunk, WS) ->
    emqx_ds_snapshot_manager:accept_chunk(Chunk, WS).

-spec complete_accept(binary(), ws()) -> ok | {error, ra_snapshot:file_err()}.
complete_accept(Chunk, WS) ->
    emqx_ds_snapshot_manager:complete_accept(Chunk, WS).

%% Restoring machine state from a snapshot.
%% This is equivalent to restoring from a log snapshot.

-spec recover(_SnapshotDir :: file:filename()) ->
    {ok, ra_snapshot:meta(), ra_state()} | {error, _Reason}.
recover(Dir) ->
    %% TODO: Verify that storage layer is online?
    ra_log_snapshot:recover(Dir).

-spec validate(_SnapshotDir :: file:filename()) ->
    ok | {error, _Reason}.
validate(Dir) ->
    ra_log_snapshot:validate(Dir).

-spec read_meta(_SnapshotDir :: file:filename()) ->
    {ok, ra_snapshot:meta()} | {error, _Reason}.
read_meta(Dir) ->
    ra_log_snapshot:read_meta(Dir).
