%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% CSV container implementation for `emqx_connector_aggregator`.
-module(emqx_connector_aggreg_csv).

%% Container API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0]).

-record(csv, {
    columns :: [binary()] | undefined,
    column_order :: [binary()],
    %% A string or character that separates each field in a record from the next.
    %% Default: ","
    field_separator :: char() | iodata(),
    %% A string or character that delimits boundaries of a record.
    %% Default: "\n"
    record_delimiter :: char() | iodata(),
    quoting_mp :: _ReMP
}).

-type container() :: #csv{}.

-type options() :: #{
    %% Which columns have to be ordered first in the resulting CSV?
    column_order => [column()]
}.

-type record() :: emqx_connector_aggregator:record().
-type column() :: binary().

%%

-spec new(options()) -> container().
new(Opts) ->
    {ok, MP} = re:compile("[\\[\\],\\r\\n\"]", [unicode]),
    #csv{
        column_order = maps:get(column_order, Opts, []),
        field_separator = $,,
        record_delimiter = $\n,
        quoting_mp = MP
    }.

-spec fill([record()], container()) -> {iodata(), container()}.
fill(Records = [Record | _], CSV0 = #csv{columns = undefined}) ->
    Columns = mk_columns(Record, CSV0),
    Header = emit_header(Columns, CSV0),
    {Writes, CSV} = fill(Records, CSV0#csv{columns = Columns}),
    {[Header | Writes], CSV};
fill(Records, CSV) ->
    Writes = [emit_row(R, CSV) || R <- Records],
    {Writes, CSV}.

-spec close(container()) -> iodata().
close(#csv{}) ->
    [].

%%

mk_columns(Record, #csv{column_order = ColumnOrder}) ->
    Columns = [emqx_utils_conv:bin(C) || C <- lists:sort(maps:keys(Record))],
    Unoredered = Columns -- ColumnOrder,
    ColumnOrder ++ Unoredered.

-spec emit_header([column()], container()) -> iodata().
emit_header([C], #csv{record_delimiter = Delim}) ->
    [C, Delim];
emit_header([C | Rest], CSV = #csv{field_separator = Sep}) ->
    [C, Sep | emit_header(Rest, CSV)];
emit_header([], #csv{record_delimiter = Delim}) ->
    [Delim].

-spec emit_row(record(), container()) -> iodata().
emit_row(Record, CSV = #csv{columns = Columns}) ->
    emit_row(Record, Columns, CSV).

emit_row(Record, [C], CSV = #csv{record_delimiter = Delim}) ->
    [emit_cell(C, Record, CSV), Delim];
emit_row(Record, [C | Rest], CSV = #csv{field_separator = Sep}) ->
    [emit_cell(C, Record, CSV), Sep | emit_row(Record, Rest, CSV)];
emit_row(#{}, [], #csv{record_delimiter = Delim}) ->
    [Delim].

emit_cell(Column, Record, CSV) ->
    case emqx_template:lookup(Column, Record) of
        {ok, Value} ->
            encode_cell(emqx_template:to_string(Value), CSV);
        {error, undefined} ->
            _Empty = ""
    end.

encode_cell(V, #csv{quoting_mp = MP}) ->
    case re:run(V, MP, []) of
        nomatch ->
            V;
        _ ->
            [$", re:replace(V, <<"\"">>, <<"\"\"">>, [global, unicode]), $"]
    end.
