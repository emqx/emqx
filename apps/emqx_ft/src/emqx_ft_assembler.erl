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

-include_lib("emqx/include/logger.hrl").

-export([start_link/3]).

-behaviour(gen_statem).
-export([callback_mode/0]).
-export([init/1]).
% -export([list_local_fragments/3]).
% -export([list_remote_fragments/3]).
% -export([start_assembling/3]).
-export([handle_event/4]).

% -export([handle_continue/2]).
% -export([handle_call/3]).
% -export([handle_cast/2]).

-record(st, {
    storage :: _Storage,
    transfer :: emqx_ft:transfer(),
    assembly :: _TODO,
    file :: {file:filename(), io:device(), term()} | undefined,
    hash,
    callback :: fun((ok | {error, term()}) -> any())
}).

-define(RPC_LIST_TIMEOUT, 1000).
-define(RPC_READSEG_TIMEOUT, 5000).

%%

start_link(Storage, Transfer, Callback) ->
    gen_statem:start_link(?MODULE, {Storage, Transfer, Callback}, []).

%%

-define(internal(C), {next_event, internal, C}).

callback_mode() ->
    handle_event_function.

init({Storage, Transfer, Callback}) ->
    St = #st{
        storage = Storage,
        transfer = Transfer,
        assembly = emqx_ft_assembly:new(),
        hash = crypto:hash_init(sha256),
        callback = Callback
    },
    {ok, list_local_fragments, St, ?internal([])}.

handle_event(internal, _, list_local_fragments, St = #st{assembly = Asm}) ->
    % TODO: what we do with non-transients errors here (e.g. `eacces`)?
    {ok, Fragments} = emqx_ft_storage_fs:list(St#st.storage, St#st.transfer, fragment),
    NAsm = emqx_ft_assembly:update(emqx_ft_assembly:append(Asm, node(), Fragments)),
    NSt = St#st{assembly = NAsm},
    case emqx_ft_assembly:status(NAsm) of
        complete ->
            {next_state, start_assembling, NSt, ?internal([])};
        {incomplete, _} ->
            Nodes = mria_mnesia:running_nodes() -- [node()],
            {next_state, {list_remote_fragments, Nodes}, NSt, ?internal([])}
        % TODO: recovery?
        % {error, _} = Reason ->
        %     {stop, Reason}
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
            {next_state, {failure, {error, Status}}, NSt, ?internal([])}
    end;
handle_event(internal, _, start_assembling, St = #st{assembly = Asm}) ->
    Filemeta = emqx_ft_assembly:filemeta(Asm),
    Coverage = emqx_ft_assembly:coverage(Asm),
    % TODO: errors
    {ok, Handle} = emqx_ft_storage_fs:open_file(St#st.storage, St#st.transfer, Filemeta),
    {next_state, {assemble, Coverage}, St#st{file = Handle}, ?internal([])};
handle_event(internal, _, {assemble, [{Node, Segment} | Rest]}, St = #st{}) ->
    % TODO
    % Currently, race is possible between getting segment info from the remote node and
    % this node garbage collecting the segment itself.
    % TODO: pipelining
    case pread(Node, Segment, St) of
        {ok, Content} ->
            case emqx_ft_storage_fs:write(St#st.file, Content) of
                {ok, NHandle} ->
                    {next_state, {assemble, Rest}, St#st{file = NHandle}, ?internal([])};
                %% TODO: better error handling
                {error, _} = Error ->
                    {next_state, {failure, Error}, St, ?internal([])}
            end;
        {error, _} = Error ->
            %% TODO: better error handling
            {next_state, {failure, Error}, St, ?internal([])}
    end;
handle_event(internal, _, {assemble, []}, St = #st{}) ->
    {next_state, complete, St, ?internal([])};
handle_event(internal, _, complete, St = #st{assembly = Asm, file = Handle, callback = Callback}) ->
    Filemeta = emqx_ft_assembly:filemeta(Asm),
    Result = emqx_ft_storage_fs:complete(St#st.storage, St#st.transfer, Filemeta, Handle),
    _ = safe_apply(Callback, Result),
    {stop, shutdown};
handle_event(internal, _, {failure, Error}, #st{callback = Callback}) ->
    _ = safe_apply(Callback, Error),
    {stop, Error}.

% handle_continue(list_local, St = #st{storage = Storage, transfer = Transfer, assembly = Asm}) ->
%     % TODO: what we do with non-transients errors here (e.g. `eacces`)?
%     {ok, Fragments} = emqx_ft_storage_fs:list(Storage, Transfer),
%     NAsm = emqx_ft_assembly:update(emqx_ft_assembly:append(Asm, node(), Fragments)),
%     NSt = St#st{assembly = NAsm},
%     case emqx_ft_assembly:status(NAsm) of
%         complete ->
%             {noreply, NSt, {continue}};
%         {more, _} ->
%             error(noimpl);
%         {error, _} ->
%             error(noimpl)
%     end,
%     {noreply, St}.

% handle_call(_Call, _From, St) ->
%     {reply, {error, badcall}, St}.

% handle_cast(_Cast, St) ->
%     {noreply, St}.

pread(Node, Segment, St) when Node =:= node() ->
    emqx_ft_storage_fs:pread(St#st.storage, St#st.transfer, Segment, 0, segsize(Segment));
pread(Node, Segment, St) ->
    emqx_ft_storage_fs_proto_v1:pread(Node, St#st.transfer, Segment, 0, segsize(Segment)).

%%

segsize(#{fragment := {segment, Info}}) ->
    maps:get(size, Info).

safe_apply(Callback, Result) ->
    try apply(Callback, [Result]) of
        _ -> ok
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "safe_apply_failed",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end.
