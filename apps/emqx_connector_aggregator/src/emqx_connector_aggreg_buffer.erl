%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This module provides pretty stupid interface for writing and reading
%% Erlang terms to/from a file descriptor (i.e. IO device), with a goal
%% of being able to write and read terms in a streaming fashion, and to
%% survive partial corruption.
%%
%% Layout of the file is as follows:
%% ```
%% ETF(Header { Metadata })
%% ETF(Record1 ByteSize)
%% ETF(Record1)
%% ETF(Record2 ByteSize)
%% ETF(Record2)
%% ...
%% ```
%% ^ ETF = Erlang External Term Format (i.e. `erlang:term_to_binary/1`).
-module(emqx_connector_aggreg_buffer).

-export([
    new_writer/2,
    write/2,
    takeover/1
]).

-export([
    new_reader/1,
    read/1
]).

-export_type([writer/0, reader/0]).

-record(reader, {
    fd :: file:io_device() | eof,
    buffer :: binary(),
    hasread = 0 :: non_neg_integer()
}).

-type writer() :: file:io_device().
-type reader() :: #reader{}.

%%

-define(VSN, 1).
-define(HEADER(MD), [?MODULE, ?VSN, MD]).

-define(READAHEAD_BYTES, 64 * 4096).
-define(SANE_TERM_SIZE, 256 * 1024 * 1024).

%%

-spec new_writer(file:io_device(), _Meta) -> writer().
new_writer(FD, Meta) ->
    %% TODO: Validate header is not too big?
    Header = term_to_iovec(?HEADER(Meta)),
    case file:write(FD, Header) of
        ok ->
            FD;
        {error, Reason} ->
            error({buffer_write_failed, Reason})
    end.

-spec write(_Term, writer()) -> ok | {error, file:posix()}.
write(Term, FD) ->
    IOData = term_to_iovec(Term),
    Marker = term_to_binary(iolist_size(IOData)),
    file:write(FD, [Marker | IOData]).

%%

-spec new_reader(file:io_device()) -> {_Meta, reader()}.
new_reader(FD) ->
    Reader0 = #reader{fd = FD, buffer = <<>>},
    Reader1 = read_buffered(?READAHEAD_BYTES, Reader0),
    case read_next_term(Reader1) of
        {?HEADER(MD), Reader} ->
            {MD, Reader};
        {UnexpectedHeader, _Reader} ->
            error({buffer_unexpected_header, UnexpectedHeader});
        eof ->
            error({buffer_incomplete, header})
    end.

-spec read(reader()) -> {_Term, reader()} | eof.
read(Reader0) ->
    case read_next_term(read_buffered(_LargeEnough = 16, Reader0)) of
        {Size, Reader1} when is_integer(Size) andalso Size > 0 andalso Size < ?SANE_TERM_SIZE ->
            case read_next_term(read_buffered(Size, Reader1)) of
                {Term, Reader} ->
                    {Term, Reader};
                eof ->
                    error({buffer_incomplete, Size})
            end;
        {UnexpectedSize, _Reader} ->
            error({buffer_unexpected_record_size, UnexpectedSize});
        eof ->
            eof
    end.

-spec takeover(reader()) -> writer().
takeover(#reader{fd = FD, hasread = HasRead}) ->
    case file:position(FD, HasRead) of
        {ok, HasRead} ->
            case file:truncate(FD) of
                ok ->
                    FD;
                {error, Reason} ->
                    error({buffer_takeover_failed, Reason})
            end;
        {error, Reason} ->
            error({buffer_takeover_failed, Reason})
    end.

read_next_term(#reader{fd = eof, buffer = <<>>}) ->
    eof;
read_next_term(Reader = #reader{buffer = Buffer, hasread = HasRead}) ->
    {Term, UsedBytes} = erlang:binary_to_term(Buffer, [safe, used]),
    BufferSize = byte_size(Buffer),
    BufferLeft = binary:part(Buffer, UsedBytes, BufferSize - UsedBytes),
    {Term, Reader#reader{buffer = BufferLeft, hasread = HasRead + UsedBytes}}.

read_buffered(_Size, Reader = #reader{fd = eof}) ->
    Reader;
read_buffered(Size, Reader = #reader{fd = FD, buffer = Buffer0}) ->
    BufferSize = byte_size(Buffer0),
    ReadSize = erlang:max(Size, ?READAHEAD_BYTES),
    case BufferSize < Size andalso file:read(FD, ReadSize) of
        false ->
            Reader;
        {ok, Data} ->
            Reader#reader{buffer = <<Buffer0/binary, Data/binary>>};
        eof ->
            Reader#reader{fd = eof};
        {error, Reason} ->
            error({buffer_read_failed, Reason})
    end.
