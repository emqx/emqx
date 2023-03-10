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

-module(emqx_ft_assembler).

-export([start_link/3]).

-behaviour(gen_statem).
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).
-export([terminate/3]).

-record(st, {
    storage :: _Storage,
    transfer :: emqx_ft:transfer(),
    assembly :: emqx_ft_assembly:t(),
    export :: _Export | undefined
}).

-define(NAME(Transfer), {n, l, {?MODULE, Transfer}}).
-define(REF(Transfer), {via, gproc, ?NAME(Transfer)}).

%%

start_link(Storage, Transfer, Size) ->
    gen_statem:start_link(?REF(Transfer), ?MODULE, {Storage, Transfer, Size}, []).

%%

-define(internal(C), {next_event, internal, C}).

callback_mode() ->
    handle_event_function.

init({Storage, Transfer, Size}) ->
    _ = erlang:process_flag(trap_exit, true),
    St = #st{
        storage = Storage,
        transfer = Transfer,
        assembly = emqx_ft_assembly:new(Size)
    },
    {ok, idle, St}.

handle_event(info, kickoff, idle, St) ->
    % NOTE
    % Someone's told us to start the work, which usually means that it has set up a monitor.
    % We could wait for this message and handle it at the end of the assembling rather than at
    % the beginning, however it would make error handling much more messier.
    {next_state, list_local_fragments, St, ?internal([])};
handle_event(internal, _, list_local_fragments, St = #st{}) ->
    % TODO: what we do with non-transients errors here (e.g. `eacces`)?
    {ok, Fragments} = emqx_ft_storage_fs:list(St#st.storage, St#st.transfer, fragment),
    NAsm = emqx_ft_assembly:update(emqx_ft_assembly:append(St#st.assembly, node(), Fragments)),
    NSt = St#st{assembly = NAsm},
    case emqx_ft_assembly:status(NAsm) of
        complete ->
            {next_state, start_assembling, NSt, ?internal([])};
        {incomplete, _} ->
            Nodes = mria_mnesia:running_nodes() -- [node()],
            {next_state, {list_remote_fragments, Nodes}, NSt, ?internal([])};
        % TODO: recovery?
        {error, _} = Error ->
            {stop, {shutdown, Error}}
    end;
handle_event(internal, _, {list_remote_fragments, Nodes}, St) ->
    % TODO
    % Async would better because we would not need to wait for some lagging nodes if
    % the coverage is already complete.
    % TODO: portable "storage" ref
    Results = emqx_ft_storage_fs_proto_v1:multilist(Nodes, St#st.transfer, fragment),
    NodeResults = lists:zip(Nodes, Results),
    NAsm = emqx_ft_assembly:update(
        lists:foldl(
            fun
                ({Node, {ok, {ok, Fragments}}}, Asm) ->
                    emqx_ft_assembly:append(Asm, Node, Fragments);
                ({_Node, _Result}, Asm) ->
                    % TODO: log?
                    Asm
            end,
            St#st.assembly,
            NodeResults
        )
    ),
    NSt = St#st{assembly = NAsm},
    case emqx_ft_assembly:status(NAsm) of
        complete ->
            {next_state, start_assembling, NSt, ?internal([])};
        % TODO: retries / recovery?
        {incomplete, _} = Status ->
            {stop, {shutdown, {error, Status}}};
        {error, _} = Error ->
            {stop, {shutdown, Error}}
    end;
handle_event(internal, _, start_assembling, St = #st{assembly = Asm}) ->
    Filemeta = emqx_ft_assembly:filemeta(Asm),
    Coverage = emqx_ft_assembly:coverage(Asm),
    % TODO: better error handling
    {ok, Export} = export_start(Filemeta, St),
    {next_state, {assemble, Coverage}, St#st{export = Export}, ?internal([])};
handle_event(internal, _, {assemble, [{Node, Segment} | Rest]}, St = #st{}) ->
    % TODO
    % Currently, race is possible between getting segment info from the remote node and
    % this node garbage collecting the segment itself.
    % TODO: pipelining
    % TODO: better error handling
    {ok, Content} = pread(Node, Segment, St),
    {ok, NExport} = export_write(St#st.export, Content),
    {next_state, {assemble, Rest}, St#st{export = NExport}, ?internal([])};
handle_event(internal, _, {assemble, []}, St = #st{}) ->
    {next_state, complete, St, ?internal([])};
handle_event(internal, _, complete, St = #st{}) ->
    Result = export_complete(St#st.export),
    ok = maybe_garbage_collect(Result, St),
    {stop, {shutdown, Result}, St#st{export = undefined}}.

terminate(_Reason, _StateName, #st{export = Export}) ->
    Export /= undefined andalso export_discard(Export).

pread(Node, Segment, St) when Node =:= node() ->
    emqx_ft_storage_fs:pread(St#st.storage, St#st.transfer, Segment, 0, segsize(Segment));
pread(Node, Segment, St) ->
    emqx_ft_storage_fs_proto_v1:pread(Node, St#st.transfer, Segment, 0, segsize(Segment)).

%%

export_start(Filemeta, #st{storage = Storage, transfer = Transfer}) ->
    {ExporterMod, Exporter} = emqx_ft_storage_fs:exporter(Storage),
    case ExporterMod:start_export(Exporter, Transfer, Filemeta) of
        {ok, Export} ->
            {ok, {ExporterMod, Export}};
        {error, _} = Error ->
            Error
    end.

export_write({ExporterMod, Export}, Content) ->
    case ExporterMod:write(Export, Content) of
        {ok, ExportNext} ->
            {ok, {ExporterMod, ExportNext}};
        {error, _} = Error ->
            Error
    end.

export_complete({ExporterMod, Export}) ->
    ExporterMod:complete(Export).

export_discard({ExporterMod, Export}) ->
    ExporterMod:discard(Export).

%%

maybe_garbage_collect(ok, #st{storage = Storage, transfer = Transfer, assembly = Asm}) ->
    Nodes = emqx_ft_assembly:nodes(Asm),
    emqx_ft_storage_fs_gc:collect(Storage, Transfer, Nodes);
maybe_garbage_collect({error, _}, _St) ->
    ok.

segsize(#{fragment := {segment, Info}}) ->
    maps:get(size, Info).
