%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_assembler).

-export([start_link/4]).

-behaviour(gen_statem).
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).
-export([terminate/3]).

-export([where/1]).

-type stdata() :: #{
    storage := emqx_ft_storage_fs:storage(),
    transfer := emqx_ft:transfer(),
    finopts := emqx_ft:finopts(),
    assembly := emqx_ft_assembly:t(),
    export => emqx_ft_storage_exporter:export()
}.

-define(NAME(Transfer), {n, l, {?MODULE, Transfer}}).
-define(REF(Transfer), {via, gproc, ?NAME(Transfer)}).

%%

start_link(Storage, Transfer, Size, Opts) ->
    gen_statem:start_link(?REF(Transfer), ?MODULE, {Storage, Transfer, Size, Opts}, []).

where(Transfer) ->
    gproc:where(?NAME(Transfer)).

%%

-type state() ::
    idle
    | list_local_fragments
    | {list_remote_fragments, [node()]}
    | start_assembling
    | {assemble, [{node(), emqx_ft_storage_fs:filefrag()}]}
    | complete.

-define(internal(C), {next_event, internal, C}).

callback_mode() ->
    handle_event_function.

-spec init(_Args) -> {ok, state(), stdata()}.
init({Storage, Transfer, Size, Opts}) ->
    _ = erlang:process_flag(trap_exit, true),
    St = #{
        storage => Storage,
        transfer => Transfer,
        finopts => Opts,
        assembly => emqx_ft_assembly:new(Size)
    },
    {ok, idle, St}.

-spec handle_event(info | internal, _, state(), stdata()) ->
    {next_state, state(), stdata(), {next_event, internal, _}}
    | {stop, {shutdown, ok | {error, _}}, stdata()}.
handle_event(info, kickoff, idle, St) ->
    % NOTE
    % Someone's told us to start the work, which usually means that it has set up a monitor.
    % We could wait for this message and handle it at the end of the assembling rather than at
    % the beginning, however it would make error handling much more messier.
    {next_state, list_local_fragments, St, ?internal([])};
handle_event(info, kickoff, _, _St) ->
    keep_state_and_data;
handle_event(
    internal,
    _,
    list_local_fragments,
    St = #{storage := Storage, transfer := Transfer, assembly := Asm}
) ->
    % TODO: what we do with non-transients errors here (e.g. `eacces`)?
    {ok, Fragments} = emqx_ft_storage_fs:list(Storage, Transfer, fragment),
    NAsm = emqx_ft_assembly:update(emqx_ft_assembly:append(Asm, node(), Fragments)),
    NSt = St#{assembly := NAsm},
    case emqx_ft_assembly:status(NAsm) of
        complete ->
            {next_state, start_assembling, NSt, ?internal([])};
        {incomplete, _} ->
            Nodes = emqx:running_nodes() -- [node()],
            {next_state, {list_remote_fragments, Nodes}, NSt, ?internal([])};
        % TODO: recovery?
        {error, _} = Error ->
            {stop, {shutdown, Error}}
    end;
handle_event(
    internal,
    _,
    {list_remote_fragments, Nodes},
    St = #{transfer := Transfer, assembly := Asm}
) ->
    % TODO
    % Async would better because we would not need to wait for some lagging nodes if
    % the coverage is already complete.
    % TODO: portable "storage" ref
    Results = emqx_ft_storage_fs_proto_v1:multilist(Nodes, Transfer, fragment),
    NodeResults = lists:zip(Nodes, Results),
    NAsm = emqx_ft_assembly:update(
        lists:foldl(
            fun
                ({Node, {ok, {ok, Fragments}}}, Acc) ->
                    emqx_ft_assembly:append(Acc, Node, Fragments);
                ({_Node, _Result}, Acc) ->
                    % TODO: log?
                    Acc
            end,
            Asm,
            NodeResults
        )
    ),
    NSt = St#{assembly := NAsm},
    case emqx_ft_assembly:status(NAsm) of
        complete ->
            {next_state, start_assembling, NSt, ?internal([])};
        % TODO: retries / recovery?
        {incomplete, _} = Status ->
            {stop, {shutdown, {error, Status}}};
        {error, _} = Error ->
            {stop, {shutdown, Error}}
    end;
handle_event(
    internal,
    _,
    start_assembling,
    St = #{storage := Storage, transfer := Transfer, assembly := Asm}
) ->
    Filemeta = emqx_ft_assembly:filemeta(Asm),
    Coverage = emqx_ft_assembly:coverage(Asm),
    case emqx_ft_storage_exporter:start_export(Storage, Transfer, Filemeta) of
        {ok, Export} ->
            {next_state, {assemble, Coverage}, St#{export => Export}, ?internal([])};
        {error, _} = Error ->
            {stop, {shutdown, Error}}
    end;
handle_event(internal, _, {assemble, [{Node, Segment} | Rest]}, St = #{export := Export}) ->
    % TODO
    % Currently, race is possible between getting segment info from the remote node and
    % this node garbage collecting the segment itself.
    % TODO: pipelining
    % TODO: better error handling
    {ok, Content} = pread(Node, Segment, St),
    case emqx_ft_storage_exporter:write(Export, Content) of
        {ok, NExport} ->
            {next_state, {assemble, Rest}, St#{export := NExport}, ?internal([])};
        {error, _} = Error ->
            {stop, {shutdown, Error}, maps:remove(export, St)}
    end;
handle_event(internal, _, {assemble, []}, St = #{}) ->
    {next_state, complete, St, ?internal([])};
handle_event(internal, _, complete, St = #{export := Export, finopts := Opts}) ->
    Result = emqx_ft_storage_exporter:complete(Export, Opts),
    _ = maybe_garbage_collect(Result, St),
    {stop, {shutdown, Result}, maps:remove(export, St)}.

-spec terminate(_Reason, state(), stdata()) -> _.
terminate(_Reason, _StateName, #{export := Export}) ->
    emqx_ft_storage_exporter:discard(Export);
terminate(_Reason, _StateName, #{}) ->
    ok.

pread(Node, Segment, #{storage := Storage, transfer := Transfer}) when Node =:= node() ->
    emqx_ft_storage_fs:pread(Storage, Transfer, Segment, 0, segsize(Segment));
pread(Node, Segment, #{transfer := Transfer}) ->
    emqx_ft_storage_fs_proto_v1:pread(Node, Transfer, Segment, 0, segsize(Segment)).

%%

maybe_garbage_collect(ok, #{storage := Storage, transfer := Transfer, assembly := Asm}) ->
    Nodes = emqx_ft_assembly:nodes(Asm),
    emqx_ft_storage_fs_gc:collect(Storage, Transfer, Nodes);
maybe_garbage_collect({error, _}, _St) ->
    ok.

segsize(#{fragment := {segment, Info}}) ->
    maps:get(size, Info).
