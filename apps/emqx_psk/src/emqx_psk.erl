%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_psk).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([ load/0
        , unload/0
        , on_psk_lookup/2
        , import/1
        ]).

-export([ start_link/0
        , stop/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(psk_entry, {psk_id :: binary(),
                    shared_secret :: binary(),
                    extra :: term()
                    }).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(TAB, ?MODULE).
-define(PSK_SHARD, emqx_psk_shard).

-define(DEFAULT_DELIMITER, <<":">>).

-define(CR, 13).
-define(LF, 10).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {rlog_shard, ?PSK_SHARD},
                {type, ordered_set},
                {disc_copies, [node()]},
                {record_name, psk_entry},
                {attributes, record_info(fields, psk_entry)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

load() ->
    emqx:hook('tls_handshake.psk_lookup', {?MODULE, on_psk_lookup, []}).

unload() ->
    emqx:unhook('tls_handshake.psk_lookup', {?MODULE, on_psk_lookup, []}).

on_psk_lookup(PSKIdentity, _UserState) ->
    case mnesia:dirty_read(?TAB, PSKIdentity) of
        [#psk_entry{shared_secret = SharedSecret}] ->
            {stop, {ok, SharedSecret}};
        _ ->
            ignore
    end.

import(SrcFile) ->
    call({import, SrcFile}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    _ = case get_config(enable) of
            true -> load();
            false -> ?SLOG(info, #{msg => "emqx_psk_disabled"})
        end,
    _ = case get_config(init_file) of
            undefined -> ok;
            InitFile -> import_psks(InitFile)
        end,
    {ok, #{}}.

handle_call({import, SrcFile}, _From, State) ->
    {reply, import_psks(SrcFile), State};

handle_call(Req, _From, State) ->
    ?SLOG(info, #{msg => "unexpected_call_discarded", req => Req}),
    {reply, {error, unexecpted}, State}.

handle_cast(Req, State) ->
    ?SLOG(info, #{msg => "unexpected_cast_discarded", req => Req}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(info, #{msg => "unexpected_info_discarded", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    unload(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_config(enable) ->
    emqx_config:get([psk, enable]);
get_config(init_file) ->
    emqx_config:get([psk, init_file], undefined);
get_config(separator) ->
    emqx_config:get([psk, separator], ?DEFAULT_DELIMITER);
get_config(chunk_size) ->
    emqx_config:get([psk, chunk_size]).

import_psks(SrcFile) ->
    case file:open(SrcFile, [read, raw, binary, read_ahead]) of
        {error, Reason} ->
            {error, Reason};
        {ok, Io} ->
            try import_psks(Io, get_config(separator), get_config(chunk_size)) of
                ok -> ok;
                {error, Reason} ->
                    ?SLOG(error, #{msg => "failed_to_import_psk_file",
                                   file => SrcFile,
                                   reason => Reason}),
                    {error, Reason}
            catch
                Class:Reason:Stacktrace ->
                    ?SLOG(error, #{msg => "failed_to_import_psk_file",
                                   file => SrcFile,
                                   class => Class,
                                   reason => Reason,
                                   stacktrace => Stacktrace}),
                    {error, Reason}
            after
                _ = file:close(Io)
            end
    end.

import_psks(Io, Delimiter, ChunkSize) ->
    case get_psks(Io, Delimiter, ChunkSize) of
        {ok, Entries} ->
            _ = trans(fun insert_psks/1, Entries),
            import_psks(Io, Delimiter, ChunkSize);
        {eof, Entries} ->
            _ = trans(fun insert_psks/1, Entries),
            ok;
        {error, Reaosn} ->
            {error, Reaosn}
    end.

get_psks(Io, Delimiter, Max) ->
    get_psks(Io, Delimiter, Max, []).

get_psks(_Io, _Delimiter, 0, Acc) ->
    {ok, Acc};
get_psks(Io, Delimiter, Remaining, Acc) ->
    case file:read_line(Io) of
        {ok, Line} ->
            case binary:split(Line, Delimiter) of
                [PSKIdentity, SharedSecret] ->
                    NSharedSecret = trim_crlf(SharedSecret),
                    get_psks(Io, Delimiter, Remaining - 1, [{PSKIdentity, NSharedSecret} | Acc]);
                _ ->
                    {error, {bad_format, Line}}
            end;
        eof ->
            {eof, Acc};
        {error, Reason} ->
            {error, Reason}
    end.

insert_psks(Entries) ->
    lists:foreach(fun(Entry) ->
                      insert_psk(Entry)
                  end, Entries).

insert_psk({PSKIdentity, SharedSecret}) ->
    mnesia:write(?TAB, #psk_entry{psk_id = PSKIdentity, shared_secret = SharedSecret}, write).

trim_crlf(Bin) ->
    Size = byte_size(Bin),
    case binary:at(Bin, Size - 1) of
        ?LF ->
            case binary:at(Bin, Size - 2) of
                ?CR -> binary:part(Bin, 0, Size - 2);
                _ -> binary:part(Bin, 0, Size - 1)
            end;
        _ -> Bin
    end.

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?PSK_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

call(Request) ->
    try
        gen_server:call(?MODULE, Request, 10000)
    catch
        exit:{timeout, _Details} ->
            {error, timeout}
    end.
