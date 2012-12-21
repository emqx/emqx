%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(file_handle_cache).

%% A File Handle Cache
%%
%% This extends a subset of the functionality of the Erlang file
%% module. In the below, we use "file handle" to specifically refer to
%% file handles, and "file descriptor" to refer to descriptors which
%% are not file handles, e.g. sockets.
%%
%% Some constraints
%% 1) This supports one writer, multiple readers per file. Nothing
%% else.
%% 2) Do not open the same file from different processes. Bad things
%% may happen, especially for writes.
%% 3) Writes are all appends. You cannot write to the middle of a
%% file, although you can truncate and then append if you want.
%% 4) Although there is a write buffer, there is no read buffer. Feel
%% free to use the read_ahead mode, but beware of the interaction
%% between that buffer and the write buffer.
%%
%% Some benefits
%% 1) You do not have to remember to call sync before close
%% 2) Buffering is much more flexible than with the plain file module,
%% and you can control when the buffer gets flushed out. This means
%% that you can rely on reads-after-writes working, without having to
%% call the expensive sync.
%% 3) Unnecessary calls to position and sync get optimised out.
%% 4) You can find out what your 'real' offset is, and what your
%% 'virtual' offset is (i.e. where the hdl really is, and where it
%% would be after the write buffer is written out).
%%
%% There is also a server component which serves to limit the number
%% of open file descriptors. This is a hard limit: the server
%% component will ensure that clients do not have more file
%% descriptors open than it's configured to allow.
%%
%% On open, the client requests permission from the server to open the
%% required number of file handles. The server may ask the client to
%% close other file handles that it has open, or it may queue the
%% request and ask other clients to close file handles they have open
%% in order to satisfy the request. Requests are always satisfied in
%% the order they arrive, even if a latter request (for a small number
%% of file handles) can be satisfied before an earlier request (for a
%% larger number of file handles). On close, the client sends a
%% message to the server. These messages allow the server to keep
%% track of the number of open handles. The client also keeps a
%% gb_tree which is updated on every use of a file handle, mapping the
%% time at which the file handle was last used (timestamp) to the
%% handle. Thus the smallest key in this tree maps to the file handle
%% that has not been used for the longest amount of time. This
%% smallest key is included in the messages to the server. As such,
%% the server keeps track of when the least recently used file handle
%% was used *at the point of the most recent open or close* by each
%% client.
%%
%% Note that this data can go very out of date, by the client using
%% the least recently used handle.
%%
%% When the limit is exceeded (i.e. the number of open file handles is
%% at the limit and there are pending 'open' requests), the server
%% calculates the average age of the last reported least recently used
%% file handle of all the clients. It then tells all the clients to
%% close any handles not used for longer than this average, by
%% invoking the callback the client registered. The client should
%% receive this message and pass it into
%% set_maximum_since_use/1. However, it is highly possible this age
%% will be greater than the ages of all the handles the client knows
%% of because the client has used its file handles in the mean
%% time. Thus at this point the client reports to the server the
%% current timestamp at which its least recently used file handle was
%% last used. The server will check two seconds later that either it
%% is back under the limit, in which case all is well again, or if
%% not, it will calculate a new average age. Its data will be much
%% more recent now, and so it is very likely that when this is
%% communicated to the clients, the clients will close file handles.
%% (In extreme cases, where it's very likely that all clients have
%% used their open handles since they last sent in an update, which
%% would mean that the average will never cause any file handles to
%% be closed, the server can send out an average age of 0, resulting
%% in all available clients closing all their file handles.)
%%
%% Care is taken to ensure that (a) processes which are blocked
%% waiting for file descriptors to become available are not sent
%% requests to close file handles; and (b) given it is known how many
%% file handles a process has open, when the average age is forced to
%% 0, close messages are only sent to enough processes to release the
%% correct number of file handles and the list of processes is
%% randomly shuffled. This ensures we don't cause processes to
%% needlessly close file handles, and ensures that we don't always
%% make such requests of the same processes.
%%
%% The advantage of this scheme is that there is only communication
%% from the client to the server on open, close, and when in the
%% process of trying to reduce file handle usage. There is no
%% communication from the client to the server on normal file handle
%% operations. This scheme forms a feed-back loop - the server does
%% not care which file handles are closed, just that some are, and it
%% checks this repeatedly when over the limit.
%%
%% Handles which are closed as a result of the server are put into a
%% "soft-closed" state in which the handle is closed (data flushed out
%% and sync'd first) but the state is maintained. The handle will be
%% fully reopened again as soon as needed, thus users of this library
%% do not need to worry about their handles being closed by the server
%% - reopening them when necessary is handled transparently.
%%
%% The server also supports obtain, release and transfer. obtain/{0,1}
%% blocks until a file descriptor is available, at which point the
%% requesting process is considered to 'own' more descriptor(s).
%% release/{0,1} is the inverse operation and releases previously obtained
%% descriptor(s). transfer/{1,2} transfers ownership of file descriptor(s)
%% between processes. It is non-blocking. Obtain has a
%% lower limit, set by the ?OBTAIN_LIMIT/1 macro. File handles can use
%% the entire limit, but will be evicted by obtain calls up to the
%% point at which no more obtain calls can be satisfied by the obtains
%% limit. Thus there will always be some capacity available for file
%% handles. Processes that use obtain are never asked to return them,
%% and they are not managed in any way by the server. It is simply a
%% mechanism to ensure that processes that need file descriptors such
%% as sockets can do so in such a way that the overall number of open
%% file descriptors is managed.
%%
%% The callers of register_callback/3, obtain, and the argument of
%% transfer are monitored, reducing the count of handles in use
%% appropriately when the processes terminate.

-behaviour(gen_server2).

-export([register_callback/3]).
-export([open/3, close/1, read/2, append/2, needs_sync/1, sync/1, position/2,
         truncate/1, current_virtual_offset/1, current_raw_offset/1, flush/1,
         copy/3, set_maximum_since_use/1, delete/1, clear/1]).
-export([obtain/0, obtain/1, release/0, release/1, transfer/1, transfer/2,
         set_limit/1, get_limit/0, info_keys/0,
         info/0, info/1]).
-export([ulimit/0]).

-export([start_link/0, start_link/2, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3, prioritise_cast/2]).

-define(SERVER, ?MODULE).
-define(RESERVED_FOR_OTHERS, 100).

-define(FILE_HANDLES_LIMIT_OTHER, 1024).
-define(FILE_HANDLES_CHECK_INTERVAL, 2000).

-define(OBTAIN_LIMIT(LIMIT), trunc((LIMIT * 0.9) - 2)).
-define(CLIENT_ETS_TABLE, file_handle_cache_client).
-define(ELDERS_ETS_TABLE, file_handle_cache_elders).

%%----------------------------------------------------------------------------

-record(file,
        { reader_count,
          has_writer
        }).

-record(handle,
        { hdl,
          offset,
          is_dirty,
          write_buffer_size,
          write_buffer_size_limit,
          write_buffer,
          at_eof,
          path,
          mode,
          options,
          is_write,
          is_read,
          last_used_at
        }).

-record(fhc_state,
        { elders,
          limit,
          open_count,
          open_pending,
          obtain_limit,
          obtain_count,
          obtain_pending,
          clients,
          timer_ref,
          alarm_set,
          alarm_clear
        }).

-record(cstate,
        { pid,
          callback,
          opened,
          obtained,
          blocked,
          pending_closes
        }).

-record(pending,
        { kind,
          pid,
          requested,
          from
        }).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ref() :: any()).
-type(ok_or_error() :: 'ok' | {'error', any()}).
-type(val_or_error(T) :: {'ok', T} | {'error', any()}).
-type(position() :: ('bof' | 'eof' | non_neg_integer() |
                     {('bof' |'eof'), non_neg_integer()} |
                     {'cur', integer()})).
-type(offset() :: non_neg_integer()).

-spec(register_callback/3 :: (atom(), atom(), [any()]) -> 'ok').
-spec(open/3 ::
        (file:filename(), [any()],
         [{'write_buffer', (non_neg_integer() | 'infinity' | 'unbuffered')}])
        -> val_or_error(ref())).
-spec(close/1 :: (ref()) -> ok_or_error()).
-spec(read/2 :: (ref(), non_neg_integer()) ->
                     val_or_error([char()] | binary()) | 'eof').
-spec(append/2 :: (ref(), iodata()) -> ok_or_error()).
-spec(sync/1 :: (ref()) ->  ok_or_error()).
-spec(position/2 :: (ref(), position()) -> val_or_error(offset())).
-spec(truncate/1 :: (ref()) -> ok_or_error()).
-spec(current_virtual_offset/1 :: (ref()) -> val_or_error(offset())).
-spec(current_raw_offset/1     :: (ref()) -> val_or_error(offset())).
-spec(flush/1 :: (ref()) -> ok_or_error()).
-spec(copy/3 :: (ref(), ref(), non_neg_integer()) ->
                     val_or_error(non_neg_integer())).
-spec(delete/1 :: (ref()) -> ok_or_error()).
-spec(clear/1 :: (ref()) -> ok_or_error()).
-spec(set_maximum_since_use/1 :: (non_neg_integer()) -> 'ok').
-spec(obtain/0 :: () -> 'ok').
-spec(obtain/1 :: (non_neg_integer()) -> 'ok').
-spec(release/0 :: () -> 'ok').
-spec(release/1 :: (non_neg_integer()) -> 'ok').
-spec(transfer/1 :: (pid()) -> 'ok').
-spec(transfer/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(set_limit/1 :: (non_neg_integer()) -> 'ok').
-spec(get_limit/0 :: () -> non_neg_integer()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/0 :: () -> rabbit_types:infos()).
-spec(info/1 :: ([atom()]) -> rabbit_types:infos()).
-spec(ulimit/0 :: () -> 'unknown' | non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------
-define(INFO_KEYS, [total_limit, total_used, sockets_limit, sockets_used]).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    start_link(fun alarm_handler:set_alarm/1, fun alarm_handler:clear_alarm/1).

start_link(AlarmSet, AlarmClear) ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [AlarmSet, AlarmClear],
                           [{timeout, infinity}]).

register_callback(M, F, A)
  when is_atom(M) andalso is_atom(F) andalso is_list(A) ->
    gen_server2:cast(?SERVER, {register_callback, self(), {M, F, A}}).

open(Path, Mode, Options) ->
    Path1 = filename:absname(Path),
    File1 = #file { reader_count = RCount, has_writer = HasWriter } =
        case get({Path1, fhc_file}) of
            File = #file {} -> File;
            undefined       -> #file { reader_count = 0,
                                       has_writer = false }
        end,
    Mode1 = append_to_write(Mode),
    IsWriter = is_writer(Mode1),
    case IsWriter andalso HasWriter of
        true  -> {error, writer_exists};
        false -> {ok, Ref} = new_closed_handle(Path1, Mode1, Options),
                 case get_or_reopen([{Ref, new}]) of
                     {ok, [_Handle1]} ->
                         RCount1 = case is_reader(Mode1) of
                                       true  -> RCount + 1;
                                       false -> RCount
                                   end,
                         HasWriter1 = HasWriter orelse IsWriter,
                         put({Path1, fhc_file},
                             File1 #file { reader_count = RCount1,
                                           has_writer = HasWriter1 }),
                         {ok, Ref};
                     Error ->
                         erase({Ref, fhc_handle}),
                         Error
                 end
    end.

close(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined -> ok;
        Handle    -> case hard_close(Handle) of
                         ok               -> ok;
                         {Error, Handle1} -> put_handle(Ref, Handle1),
                                             Error
                     end
    end.

read(Ref, Count) ->
    with_flushed_handles(
      [Ref],
      fun ([#handle { is_read = false }]) ->
              {error, not_open_for_reading};
          ([Handle = #handle { hdl = Hdl, offset = Offset }]) ->
              case prim_file:read(Hdl, Count) of
                  {ok, Data} = Obj -> Offset1 = Offset + iolist_size(Data),
                                      {Obj,
                                       [Handle #handle { offset = Offset1 }]};
                  eof              -> {eof, [Handle #handle { at_eof = true }]};
                  Error            -> {Error, [Handle]}
              end
      end).

append(Ref, Data) ->
    with_handles(
      [Ref],
      fun ([#handle { is_write = false }]) ->
              {error, not_open_for_writing};
          ([Handle]) ->
              case maybe_seek(eof, Handle) of
                  {{ok, _Offset}, #handle { hdl = Hdl, offset = Offset,
                                            write_buffer_size_limit = 0,
                                            at_eof = true } = Handle1} ->
                      Offset1 = Offset + iolist_size(Data),
                      {prim_file:write(Hdl, Data),
                       [Handle1 #handle { is_dirty = true, offset = Offset1 }]};
                  {{ok, _Offset}, #handle { write_buffer = WriteBuffer,
                                            write_buffer_size = Size,
                                            write_buffer_size_limit = Limit,
                                            at_eof = true } = Handle1} ->
                      WriteBuffer1 = [Data | WriteBuffer],
                      Size1 = Size + iolist_size(Data),
                      Handle2 = Handle1 #handle { write_buffer = WriteBuffer1,
                                                  write_buffer_size = Size1 },
                      case Limit =/= infinity andalso Size1 > Limit of
                          true  -> {Result, Handle3} = write_buffer(Handle2),
                                   {Result, [Handle3]};
                          false -> {ok, [Handle2]}
                      end;
                  {{error, _} = Error, Handle1} ->
                      {Error, [Handle1]}
              end
      end).

sync(Ref) ->
    with_flushed_handles(
      [Ref],
      fun ([#handle { is_dirty = false, write_buffer = [] }]) ->
              ok;
          ([Handle = #handle { hdl = Hdl,
                               is_dirty = true, write_buffer = [] }]) ->
              case prim_file:sync(Hdl) of
                  ok    -> {ok, [Handle #handle { is_dirty = false }]};
                  Error -> {Error, [Handle]}
              end
      end).

needs_sync(Ref) ->
    %% This must *not* use with_handles/2; see bug 25052
    case get({Ref, fhc_handle}) of
        #handle { is_dirty = false, write_buffer = [] } -> false;
        #handle {}                                      -> true
    end.

position(Ref, NewOffset) ->
    with_flushed_handles(
      [Ref],
      fun ([Handle]) -> {Result, Handle1} = maybe_seek(NewOffset, Handle),
                        {Result, [Handle1]}
      end).

truncate(Ref) ->
    with_flushed_handles(
      [Ref],
      fun ([Handle1 = #handle { hdl = Hdl }]) ->
              case prim_file:truncate(Hdl) of
                  ok    -> {ok, [Handle1 #handle { at_eof = true }]};
                  Error -> {Error, [Handle1]}
              end
      end).

current_virtual_offset(Ref) ->
    with_handles([Ref], fun ([#handle { at_eof = true, is_write = true,
                                        offset = Offset,
                                        write_buffer_size = Size }]) ->
                                {ok, Offset + Size};
                            ([#handle { offset = Offset }]) ->
                                {ok, Offset}
                        end).

current_raw_offset(Ref) ->
    with_handles([Ref], fun ([Handle]) -> {ok, Handle #handle.offset} end).

flush(Ref) ->
    with_flushed_handles([Ref], fun ([Handle]) -> {ok, [Handle]} end).

copy(Src, Dest, Count) ->
    with_flushed_handles(
      [Src, Dest],
      fun ([SHandle = #handle { is_read  = true, hdl = SHdl, offset = SOffset },
            DHandle = #handle { is_write = true, hdl = DHdl, offset = DOffset }]
          ) ->
              case prim_file:copy(SHdl, DHdl, Count) of
                  {ok, Count1} = Result1 ->
                      {Result1,
                       [SHandle #handle { offset = SOffset + Count1 },
                        DHandle #handle { offset = DOffset + Count1,
                                          is_dirty = true }]};
                  Error ->
                      {Error, [SHandle, DHandle]}
              end;
          (_Handles) ->
              {error, incorrect_handle_modes}
      end).

delete(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined ->
            ok;
        Handle = #handle { path = Path } ->
            case hard_close(Handle #handle { is_dirty = false,
                                             write_buffer = [] }) of
                ok               -> prim_file:delete(Path);
                {Error, Handle1} -> put_handle(Ref, Handle1),
                                    Error
            end
    end.

clear(Ref) ->
    with_handles(
      [Ref],
      fun ([#handle { at_eof = true, write_buffer_size = 0, offset = 0 }]) ->
              ok;
          ([Handle]) ->
              case maybe_seek(bof, Handle #handle { write_buffer = [],
                                                    write_buffer_size = 0 }) of
                  {{ok, 0}, Handle1 = #handle { hdl = Hdl }} ->
                      case prim_file:truncate(Hdl) of
                          ok    -> {ok, [Handle1 #handle { at_eof = true }]};
                          Error -> {Error, [Handle1]}
                      end;
                  {{error, _} = Error, Handle1} ->
                      {Error, [Handle1]}
              end
      end).

set_maximum_since_use(MaximumAge) ->
    Now = now(),
    case lists:foldl(
           fun ({{Ref, fhc_handle},
                 Handle = #handle { hdl = Hdl, last_used_at = Then }}, Rep) ->
                   case Hdl =/= closed andalso
                       timer:now_diff(Now, Then) >= MaximumAge of
                       true  -> soft_close(Ref, Handle) orelse Rep;
                       false -> Rep
                   end;
               (_KeyValuePair, Rep) ->
                   Rep
           end, false, get()) of
        false -> age_tree_change(), ok;
        true  -> ok
    end.

obtain()      -> obtain(1).
release()     -> release(1).
transfer(Pid) -> transfer(Pid, 1).

obtain(Count) when Count > 0 ->
    %% If the FHC isn't running, obtains succeed immediately.
    case whereis(?SERVER) of
        undefined -> ok;
        _         -> gen_server2:call(?SERVER, {obtain, Count, self()}, infinity)
    end.

release(Count) when Count > 0 ->
    gen_server2:cast(?SERVER, {release, Count, self()}).

transfer(Pid, Count) when Count > 0 ->
    gen_server2:cast(?SERVER, {transfer, Count, self(), Pid}).

set_limit(Limit) ->
    gen_server2:call(?SERVER, {set_limit, Limit}, infinity).

get_limit() ->
    gen_server2:call(?SERVER, get_limit, infinity).

info_keys() -> ?INFO_KEYS.

info() -> info(?INFO_KEYS).
info(Items) -> gen_server2:call(?SERVER, {info, Items}, infinity).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

is_reader(Mode) -> lists:member(read, Mode).

is_writer(Mode) -> lists:member(write, Mode).

append_to_write(Mode) ->
    case lists:member(append, Mode) of
        true  -> [write | Mode -- [append, write]];
        false -> Mode
    end.

with_handles(Refs, Fun) ->
    case get_or_reopen([{Ref, reopen} || Ref <- Refs]) of
        {ok, Handles} ->
            case Fun(Handles) of
                {Result, Handles1} when is_list(Handles1) ->
                    lists:zipwith(fun put_handle/2, Refs, Handles1),
                    Result;
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

with_flushed_handles(Refs, Fun) ->
    with_handles(
      Refs,
      fun (Handles) ->
              case lists:foldl(
                     fun (Handle, {ok, HandlesAcc}) ->
                             {Res, Handle1} = write_buffer(Handle),
                             {Res, [Handle1 | HandlesAcc]};
                         (Handle, {Error, HandlesAcc}) ->
                             {Error, [Handle | HandlesAcc]}
                     end, {ok, []}, Handles) of
                  {ok, Handles1} ->
                      Fun(lists:reverse(Handles1));
                  {Error, Handles1} ->
                      {Error, lists:reverse(Handles1)}
              end
      end).

get_or_reopen(RefNewOrReopens) ->
    case partition_handles(RefNewOrReopens) of
        {OpenHdls, []} ->
            {ok, [Handle || {_Ref, Handle} <- OpenHdls]};
        {OpenHdls, ClosedHdls} ->
            Oldest = oldest(get_age_tree(), fun () -> now() end),
            case gen_server2:call(?SERVER, {open, self(), length(ClosedHdls),
                                            Oldest}, infinity) of
                ok ->
                    case reopen(ClosedHdls) of
                        {ok, RefHdls}  -> sort_handles(RefNewOrReopens,
                                                       OpenHdls, RefHdls, []);
                        Error          -> Error
                    end;
                close ->
                    [soft_close(Ref, Handle) ||
                        {{Ref, fhc_handle}, Handle = #handle { hdl = Hdl }} <-
                            get(),
                        Hdl =/= closed],
                    get_or_reopen(RefNewOrReopens)
            end
    end.

reopen(ClosedHdls) -> reopen(ClosedHdls, get_age_tree(), []).

reopen([], Tree, RefHdls) ->
    put_age_tree(Tree),
    {ok, lists:reverse(RefHdls)};
reopen([{Ref, NewOrReopen, Handle = #handle { hdl          = closed,
                                              path         = Path,
                                              mode         = Mode,
                                              offset       = Offset,
                                              last_used_at = undefined }} |
        RefNewOrReopenHdls] = ToOpen, Tree, RefHdls) ->
    case prim_file:open(Path, case NewOrReopen of
                                  new    -> Mode;
                                  reopen -> [read | Mode]
                              end) of
        {ok, Hdl} ->
            Now = now(),
            {{ok, _Offset}, Handle1} =
                maybe_seek(Offset, Handle #handle { hdl          = Hdl,
                                                    offset       = 0,
                                                    last_used_at = Now }),
            put({Ref, fhc_handle}, Handle1),
            reopen(RefNewOrReopenHdls, gb_trees:insert(Now, Ref, Tree),
                   [{Ref, Handle1} | RefHdls]);
        Error ->
            %% NB: none of the handles in ToOpen are in the age tree
            Oldest = oldest(Tree, fun () -> undefined end),
            [gen_server2:cast(?SERVER, {close, self(), Oldest}) || _ <- ToOpen],
            put_age_tree(Tree),
            Error
    end.

partition_handles(RefNewOrReopens) ->
    lists:foldr(
      fun ({Ref, NewOrReopen}, {Open, Closed}) ->
              case get({Ref, fhc_handle}) of
                  #handle { hdl = closed } = Handle ->
                      {Open, [{Ref, NewOrReopen, Handle} | Closed]};
                  #handle {} = Handle ->
                      {[{Ref, Handle} | Open], Closed}
              end
      end, {[], []}, RefNewOrReopens).

sort_handles([], [], [], Acc) ->
    {ok, lists:reverse(Acc)};
sort_handles([{Ref, _} | RefHdls], [{Ref, Handle} | RefHdlsA], RefHdlsB, Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]);
sort_handles([{Ref, _} | RefHdls], RefHdlsA, [{Ref, Handle} | RefHdlsB], Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]).

put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
    Now = now(),
    age_tree_update(Then, Now, Ref),
    put({Ref, fhc_handle}, Handle #handle { last_used_at = Now }).

with_age_tree(Fun) -> put_age_tree(Fun(get_age_tree())).

get_age_tree() ->
    case get(fhc_age_tree) of
        undefined -> gb_trees:empty();
        AgeTree   -> AgeTree
    end.

put_age_tree(Tree) -> put(fhc_age_tree, Tree).

age_tree_update(Then, Now, Ref) ->
    with_age_tree(
      fun (Tree) ->
              gb_trees:insert(Now, Ref, gb_trees:delete_any(Then, Tree))
      end).

age_tree_delete(Then) ->
    with_age_tree(
      fun (Tree) ->
              Tree1 = gb_trees:delete_any(Then, Tree),
              Oldest = oldest(Tree1, fun () -> undefined end),
              gen_server2:cast(?SERVER, {close, self(), Oldest}),
              Tree1
      end).

age_tree_change() ->
    with_age_tree(
      fun (Tree) ->
              case gb_trees:is_empty(Tree) of
                  true  -> Tree;
                  false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
                           gen_server2:cast(?SERVER, {update, self(), Oldest})
              end,
              Tree
      end).

oldest(Tree, DefaultFun) ->
    case gb_trees:is_empty(Tree) of
        true  -> DefaultFun();
        false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
                 Oldest
    end.

new_closed_handle(Path, Mode, Options) ->
    WriteBufferSize =
        case proplists:get_value(write_buffer, Options, unbuffered) of
            unbuffered           -> 0;
            infinity             -> infinity;
            N when is_integer(N) -> N
        end,
    Ref = make_ref(),
    put({Ref, fhc_handle}, #handle { hdl                     = closed,
                                     offset                  = 0,
                                     is_dirty                = false,
                                     write_buffer_size       = 0,
                                     write_buffer_size_limit = WriteBufferSize,
                                     write_buffer            = [],
                                     at_eof                  = false,
                                     path                    = Path,
                                     mode                    = Mode,
                                     options                 = Options,
                                     is_write                = is_writer(Mode),
                                     is_read                 = is_reader(Mode),
                                     last_used_at            = undefined }),
    {ok, Ref}.

soft_close(Ref, Handle) ->
    {Res, Handle1} = soft_close(Handle),
    case Res of
        ok -> put({Ref, fhc_handle}, Handle1),
              true;
        _  -> put_handle(Ref, Handle1),
              false
    end.

soft_close(Handle = #handle { hdl = closed }) ->
    {ok, Handle};
soft_close(Handle) ->
    case write_buffer(Handle) of
        {ok, #handle { hdl         = Hdl,
                       is_dirty    = IsDirty,
                       last_used_at = Then } = Handle1 } ->
            ok = case IsDirty of
                     true  -> prim_file:sync(Hdl);
                     false -> ok
                 end,
            ok = prim_file:close(Hdl),
            age_tree_delete(Then),
            {ok, Handle1 #handle { hdl            = closed,
                                   is_dirty       = false,
                                   last_used_at   = undefined }};
        {_Error, _Handle} = Result ->
            Result
    end.

hard_close(Handle) ->
    case soft_close(Handle) of
        {ok, #handle { path = Path,
                       is_read = IsReader, is_write = IsWriter }} ->
            #file { reader_count = RCount, has_writer = HasWriter } = File =
                get({Path, fhc_file}),
            RCount1 = case IsReader of
                          true  -> RCount - 1;
                          false -> RCount
                      end,
            HasWriter1 = HasWriter andalso not IsWriter,
            case RCount1 =:= 0 andalso not HasWriter1 of
                true  -> erase({Path, fhc_file});
                false -> put({Path, fhc_file},
                             File #file { reader_count = RCount1,
                                          has_writer = HasWriter1 })
            end,
            ok;
        {_Error, _Handle} = Result ->
            Result
    end.

maybe_seek(NewOffset, Handle = #handle { hdl = Hdl, offset = Offset,
                                         at_eof = AtEoF }) ->
    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Offset, NewOffset),
    case (case NeedsSeek of
              true  -> prim_file:position(Hdl, NewOffset);
              false -> {ok, Offset}
          end) of
        {ok, Offset1} = Result ->
            {Result, Handle #handle { offset = Offset1, at_eof = AtEoF1 }};
        {error, _} = Error ->
            {Error, Handle}
    end.

needs_seek( AtEoF, _CurOffset,  cur     ) -> {AtEoF, false};
needs_seek( AtEoF, _CurOffset,  {cur, 0}) -> {AtEoF, false};
needs_seek(  true, _CurOffset,  eof     ) -> {true , false};
needs_seek(  true, _CurOffset,  {eof, 0}) -> {true , false};
needs_seek( false, _CurOffset,  eof     ) -> {true , true };
needs_seek( false, _CurOffset,  {eof, 0}) -> {true , true };
needs_seek( AtEoF,          0,  bof     ) -> {AtEoF, false};
needs_seek( AtEoF,          0,  {bof, 0}) -> {AtEoF, false};
needs_seek( AtEoF,  CurOffset, CurOffset) -> {AtEoF, false};
needs_seek(  true,  CurOffset, {bof, DesiredOffset})
  when DesiredOffset >= CurOffset ->
    {true, true};
needs_seek(  true, _CurOffset, {cur, DesiredOffset})
  when DesiredOffset > 0 ->
    {true, true};
needs_seek(  true,  CurOffset, DesiredOffset) %% same as {bof, DO}
  when is_integer(DesiredOffset) andalso DesiredOffset >= CurOffset ->
    {true, true};
%% because we can't really track size, we could well end up at EoF and not know
needs_seek(_AtEoF, _CurOffset, _DesiredOffset) ->
    {false, true}.

write_buffer(Handle = #handle { write_buffer = [] }) ->
    {ok, Handle};
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
                                write_buffer = WriteBuffer,
                                write_buffer_size = DataSize,
                                at_eof = true }) ->
    case prim_file:write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = Offset + DataSize,
            {ok, Handle #handle { offset = Offset1, is_dirty = true,
                                  write_buffer = [], write_buffer_size = 0 }};
        {error, _} = Error ->
            {Error, Handle}
    end.

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(total_limit,   #fhc_state{limit        = Limit})               -> Limit;
i(total_used,    #fhc_state{open_count = C1, obtain_count = C2}) -> C1 + C2;
i(sockets_limit, #fhc_state{obtain_limit = Limit})               -> Limit;
i(sockets_used,  #fhc_state{obtain_count = Count})               -> Count;
i(Item, _) -> throw({bad_argument, Item}).

%%----------------------------------------------------------------------------
%% gen_server2 callbacks
%%----------------------------------------------------------------------------

init([AlarmSet, AlarmClear]) ->
    Limit = case application:get_env(file_handles_high_watermark) of
                {ok, Watermark} when (is_integer(Watermark) andalso
                                      Watermark > 0) ->
                    Watermark;
                _ ->
                    case ulimit() of
                        unknown  -> ?FILE_HANDLES_LIMIT_OTHER;
                        Lim      -> lists:max([2, Lim - ?RESERVED_FOR_OTHERS])
                    end
            end,
    ObtainLimit = obtain_limit(Limit),
    error_logger:info_msg("Limiting to approx ~p file handles (~p sockets)~n",
                          [Limit, ObtainLimit]),
    Clients = ets:new(?CLIENT_ETS_TABLE, [set, private, {keypos, #cstate.pid}]),
    Elders = ets:new(?ELDERS_ETS_TABLE, [set, private]),
    {ok, #fhc_state { elders         = Elders,
                      limit          = Limit,
                      open_count     = 0,
                      open_pending   = pending_new(),
                      obtain_limit   = ObtainLimit,
                      obtain_count   = 0,
                      obtain_pending = pending_new(),
                      clients        = Clients,
                      timer_ref      = undefined,
                      alarm_set      = AlarmSet,
                      alarm_clear    = AlarmClear }}.

prioritise_cast(Msg, _State) ->
    case Msg of
        {release, _, _}              -> 5;
        _                            -> 0
    end.

handle_call({open, Pid, Requested, EldestUnusedSince}, From,
            State = #fhc_state { open_count   = Count,
                                 open_pending = Pending,
                                 elders       = Elders,
                                 clients      = Clients })
  when EldestUnusedSince =/= undefined ->
    true = ets:insert(Elders, {Pid, EldestUnusedSince}),
    Item = #pending { kind      = open,
                      pid       = Pid,
                      requested = Requested,
                      from      = From },
    ok = track_client(Pid, Clients),
    case needs_reduce(State #fhc_state { open_count = Count + Requested }) of
        true  -> case ets:lookup(Clients, Pid) of
                     [#cstate { opened = 0 }] ->
                         true = ets:update_element(
                                  Clients, Pid, {#cstate.blocked, true}),
                         {noreply,
                          reduce(State #fhc_state {
                                   open_pending = pending_in(Item, Pending) })};
                     [#cstate { opened = Opened }] ->
                         true = ets:update_element(
                                  Clients, Pid,
                                  {#cstate.pending_closes, Opened}),
                         {reply, close, State}
                 end;
        false -> {noreply, run_pending_item(Item, State)}
    end;

handle_call({obtain, N, Pid}, From, State = #fhc_state {
                                              obtain_count   = Count,
                                              obtain_pending = Pending,
                                              clients = Clients }) ->
    ok = track_client(Pid, Clients),
    Item = #pending { kind = obtain, pid = Pid, requested = N, from = From },
    Enqueue = fun () ->
                      true = ets:update_element(Clients, Pid,
                                                {#cstate.blocked, true}),
                      State #fhc_state {
                        obtain_pending = pending_in(Item, Pending) }
              end,
    {noreply,
        case obtain_limit_reached(State) of
            true  -> Enqueue();
            false -> case needs_reduce(State #fhc_state {
                                      obtain_count = Count + N }) of
                         true  -> reduce(Enqueue());
                         false -> adjust_alarm(
                                      State, run_pending_item(Item, State))
                     end
        end};

handle_call({set_limit, Limit}, _From, State) ->
    {reply, ok, adjust_alarm(
                  State, maybe_reduce(
                           process_pending(
                             State #fhc_state {
                               limit        = Limit,
                               obtain_limit = obtain_limit(Limit) })))};

handle_call(get_limit, _From, State = #fhc_state { limit = Limit }) ->
    {reply, Limit, State};

handle_call({info, Items}, _From, State) ->
    {reply, infos(Items, State), State}.

handle_cast({register_callback, Pid, MFA},
            State = #fhc_state { clients = Clients }) ->
    ok = track_client(Pid, Clients),
    true = ets:update_element(Clients, Pid, {#cstate.callback, MFA}),
    {noreply, State};

handle_cast({update, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders })
  when EldestUnusedSince =/= undefined ->
    true = ets:insert(Elders, {Pid, EldestUnusedSince}),
    %% don't call maybe_reduce from here otherwise we can create a
    %% storm of messages
    {noreply, State};

handle_cast({release, N, Pid}, State) ->
    {noreply, adjust_alarm(State, process_pending(
                                    update_counts(obtain, Pid, -N, State)))};

handle_cast({close, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders, clients = Clients }) ->
    true = case EldestUnusedSince of
               undefined -> ets:delete(Elders, Pid);
               _         -> ets:insert(Elders, {Pid, EldestUnusedSince})
           end,
    ets:update_counter(Clients, Pid, {#cstate.pending_closes, -1, 0, 0}),
    {noreply, adjust_alarm(State, process_pending(
                update_counts(open, Pid, -1, State)))};

handle_cast({transfer, N, FromPid, ToPid}, State) ->
    ok = track_client(ToPid, State#fhc_state.clients),
    {noreply, process_pending(
                update_counts(obtain, ToPid, +N,
                              update_counts(obtain, FromPid, -N, State)))}.

handle_info(check_counts, State) ->
    {noreply, maybe_reduce(State #fhc_state { timer_ref = undefined })};

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #fhc_state { elders         = Elders,
                                 open_count     = OpenCount,
                                 open_pending   = OpenPending,
                                 obtain_count   = ObtainCount,
                                 obtain_pending = ObtainPending,
                                 clients        = Clients }) ->
    [#cstate { opened = Opened, obtained = Obtained }] =
        ets:lookup(Clients, Pid),
    true = ets:delete(Clients, Pid),
    true = ets:delete(Elders, Pid),
    FilterFun = fun (#pending { pid = Pid1 }) -> Pid1 =/= Pid end,
    {noreply, adjust_alarm(
                State,
                process_pending(
                  State #fhc_state {
                    open_count     = OpenCount - Opened,
                    open_pending   = filter_pending(FilterFun, OpenPending),
                    obtain_count   = ObtainCount - Obtained,
                    obtain_pending = filter_pending(FilterFun, ObtainPending) }))}.

terminate(_Reason, State = #fhc_state { clients = Clients,
                                        elders  = Elders }) ->
    ets:delete(Clients),
    ets:delete(Elders),
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% pending queue abstraction helpers
%%----------------------------------------------------------------------------

queue_fold(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

filter_pending(Fun, {Count, Queue}) ->
    {Delta, Queue1} =
        queue_fold(
          fun (Item = #pending { requested = Requested }, {DeltaN, QueueN}) ->
                  case Fun(Item) of
                      true  -> {DeltaN, queue:in(Item, QueueN)};
                      false -> {DeltaN - Requested, QueueN}
                  end
          end, {0, queue:new()}, Queue),
    {Count + Delta, Queue1}.

pending_new() ->
    {0, queue:new()}.

pending_in(Item = #pending { requested = Requested }, {Count, Queue}) ->
    {Count + Requested, queue:in(Item, Queue)}.

pending_out({0, _Queue} = Pending) ->
    {empty, Pending};
pending_out({N, Queue}) ->
    {{value, #pending { requested = Requested }} = Result, Queue1} =
        queue:out(Queue),
    {Result, {N - Requested, Queue1}}.

pending_count({Count, _Queue}) ->
    Count.

pending_is_empty({0, _Queue}) ->
    true;
pending_is_empty({_N, _Queue}) ->
    false.

%%----------------------------------------------------------------------------
%% server helpers
%%----------------------------------------------------------------------------

obtain_limit(infinity) -> infinity;
obtain_limit(Limit)    -> case ?OBTAIN_LIMIT(Limit) of
                              OLimit when OLimit < 0 -> 0;
                              OLimit                 -> OLimit
                          end.

obtain_limit_reached(#fhc_state { obtain_limit = Limit,
                                  obtain_count = Count}) ->
    Limit =/= infinity andalso Count >= Limit.

adjust_alarm(OldState = #fhc_state { alarm_set   = AlarmSet,
                                     alarm_clear = AlarmClear }, NewState) ->
    case {obtain_limit_reached(OldState), obtain_limit_reached(NewState)} of
        {false, true} -> AlarmSet({file_descriptor_limit, []});
        {true, false} -> AlarmClear(file_descriptor_limit);
        _             -> ok
    end,
    NewState.

process_pending(State = #fhc_state { limit = infinity }) ->
    State;
process_pending(State) ->
    process_open(process_obtain(State)).

process_open(State = #fhc_state { limit        = Limit,
                                  open_pending = Pending,
                                  open_count   = OpenCount,
                                  obtain_count = ObtainCount }) ->
    {Pending1, State1} =
        process_pending(Pending, Limit - (ObtainCount + OpenCount), State),
    State1 #fhc_state { open_pending = Pending1 }.

process_obtain(State = #fhc_state { limit          = Limit,
                                    obtain_pending = Pending,
                                    obtain_limit   = ObtainLimit,
                                    obtain_count   = ObtainCount,
                                    open_count     = OpenCount }) ->
    Quota = lists:min([ObtainLimit - ObtainCount,
                       Limit - (ObtainCount + OpenCount)]),
    {Pending1, State1} = process_pending(Pending, Quota, State),
    State1 #fhc_state { obtain_pending = Pending1 }.

process_pending(Pending, Quota, State) when Quota =< 0 ->
    {Pending, State};
process_pending(Pending, Quota, State) ->
    case pending_out(Pending) of
        {empty, _Pending} ->
            {Pending, State};
        {{value, #pending { requested = Requested }}, _Pending1}
          when Requested > Quota ->
            {Pending, State};
        {{value, #pending { requested = Requested } = Item}, Pending1} ->
            process_pending(Pending1, Quota - Requested,
                            run_pending_item(Item, State))
    end.

run_pending_item(#pending { kind      = Kind,
                            pid       = Pid,
                            requested = Requested,
                            from      = From },
                 State = #fhc_state { clients = Clients }) ->
    gen_server2:reply(From, ok),
    true = ets:update_element(Clients, Pid, {#cstate.blocked, false}),
    update_counts(Kind, Pid, Requested, State).

update_counts(Kind, Pid, Delta,
              State = #fhc_state { open_count   = OpenCount,
                                   obtain_count = ObtainCount,
                                   clients      = Clients }) ->
    {OpenDelta, ObtainDelta} = update_counts1(Kind, Pid, Delta, Clients),
    State #fhc_state { open_count   = OpenCount   + OpenDelta,
                       obtain_count = ObtainCount + ObtainDelta }.

update_counts1(open, Pid, Delta, Clients) ->
    ets:update_counter(Clients, Pid, {#cstate.opened, Delta}),
    {Delta, 0};
update_counts1(obtain, Pid, Delta, Clients) ->
    ets:update_counter(Clients, Pid, {#cstate.obtained, Delta}),
    {0, Delta}.

maybe_reduce(State) ->
    case needs_reduce(State) of
        true  -> reduce(State);
        false -> State
    end.

needs_reduce(#fhc_state { limit          = Limit,
                          open_count     = OpenCount,
                          open_pending   = OpenPending,
                          obtain_count   = ObtainCount,
                          obtain_limit   = ObtainLimit,
                          obtain_pending = ObtainPending }) ->
    Limit =/= infinity
        andalso ((OpenCount + ObtainCount > Limit)
                 orelse (not pending_is_empty(OpenPending))
                 orelse (ObtainCount < ObtainLimit
                         andalso not pending_is_empty(ObtainPending))).

reduce(State = #fhc_state { open_pending   = OpenPending,
                            obtain_pending = ObtainPending,
                            elders         = Elders,
                            clients        = Clients,
                            timer_ref      = TRef }) ->
    Now = now(),
    {CStates, Sum, ClientCount} =
        ets:foldl(fun ({Pid, Eldest}, {CStatesAcc, SumAcc, CountAcc} = Accs) ->
                          [#cstate { pending_closes = PendingCloses,
                                     opened         = Opened,
                                     blocked        = Blocked } = CState] =
                              ets:lookup(Clients, Pid),
                          case Blocked orelse PendingCloses =:= Opened of
                              true  -> Accs;
                              false -> {[CState | CStatesAcc],
                                        SumAcc + timer:now_diff(Now, Eldest),
                                        CountAcc + 1}
                          end
                  end, {[], 0, 0}, Elders),
    case CStates of
        [] -> ok;
        _  -> case (Sum / ClientCount) -
                  (1000 * ?FILE_HANDLES_CHECK_INTERVAL) of
                  AverageAge when AverageAge > 0 ->
                      notify_age(CStates, AverageAge);
                  _ ->
                      notify_age0(Clients, CStates,
                                  pending_count(OpenPending) +
                                      pending_count(ObtainPending))
              end
    end,
    case TRef of
        undefined -> TRef1 = erlang:send_after(
                               ?FILE_HANDLES_CHECK_INTERVAL, ?SERVER,
                               check_counts),
                     State #fhc_state { timer_ref = TRef1 };
        _         -> State
    end.

notify_age(CStates, AverageAge) ->
    lists:foreach(
      fun (#cstate { callback = undefined }) -> ok;
          (#cstate { callback = {M, F, A} }) -> apply(M, F, A ++ [AverageAge])
      end, CStates).

notify_age0(Clients, CStates, Required) ->
    case [CState || CState <- CStates, CState#cstate.callback =/= undefined] of
        []            -> ok;
        Notifications -> S = random:uniform(length(Notifications)),
                         {L1, L2} = lists:split(S, Notifications),
                         notify(Clients, Required, L2 ++ L1)
    end.

notify(_Clients, _Required, []) ->
    ok;
notify(_Clients, Required, _Notifications) when Required =< 0 ->
    ok;
notify(Clients, Required, [#cstate{ pid      = Pid,
                                    callback = {M, F, A},
                                    opened   = Opened } | Notifications]) ->
    apply(M, F, A ++ [0]),
    ets:update_element(Clients, Pid, {#cstate.pending_closes, Opened}),
    notify(Clients, Required - Opened, Notifications).

track_client(Pid, Clients) ->
    case ets:insert_new(Clients, #cstate { pid            = Pid,
                                           callback       = undefined,
                                           opened         = 0,
                                           obtained       = 0,
                                           blocked        = false,
                                           pending_closes = 0 }) of
        true  -> _MRef = erlang:monitor(process, Pid),
                 ok;
        false -> ok
    end.


%% To increase the number of file descriptors: on Windows set ERL_MAX_PORTS
%% environment variable, on Linux set `ulimit -n`.
ulimit() ->
    case proplists:get_value(max_fds, erlang:system_info(check_io)) of
        MaxFds when is_integer(MaxFds) andalso MaxFds > 1 ->
            case os:type() of
                {win32, _OsName} ->
                    %% On Windows max_fds is twice the number of open files:
                    %%   https://github.com/yrashk/erlang/blob/e1282325ed75e52a98d5/erts/emulator/sys/win32/sys.c#L2459-2466
                    MaxFds div 2;
                _Any ->
                    %% For other operating systems trust Erlang.
                    MaxFds
            end;
        _ ->
            unknown
    end.
