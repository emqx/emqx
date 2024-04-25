%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% CSV container implementation for `emqx_bridge_s3_aggregator`.
-module(emqx_bridge_s3_aggreg_csv).

%% Container API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0]).

-record(csv, {
    columns :: [binary()] | undefined,
    order :: [binary()],
    separator :: char() | iodata(),
    delimiter :: char() | iodata(),
    quoting_mp :: _ReMP
}).

-type container() :: #csv{}.
-type options() :: #{column_order => [column()]}.

-type record() :: emqx_bridge_s3_aggregator:record().
-type column() :: binary().

%%

-spec new(options()) -> container().
new(Opts) ->
    {ok, MP} = re:compile("[\\[\\],\\r\\n\"]", [unicode]),
    #csv{
        order = maps:get(column_order, Opts, []),
        separator = $,,
        delimiter = $\n,
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

mk_columns(Record, #csv{order = ColumnOrder}) ->
    Columns = lists:sort(maps:keys(Record)),
    OrderedFirst = [CO || CO <- ColumnOrder, lists:member(CO, Columns)],
    Unoredered = Columns -- ColumnOrder,
    OrderedFirst ++ Unoredered.

-spec emit_header([column()], container()) -> iodata().
emit_header([C], #csv{delimiter = Delim}) ->
    [C, Delim];
emit_header([C | Rest], CSV = #csv{separator = Sep}) ->
    [C, Sep | emit_header(Rest, CSV)];
emit_header([], #csv{delimiter = Delim}) ->
    [Delim].

-spec emit_row(record(), container()) -> iodata().
emit_row(Record, CSV = #csv{columns = Columns}) ->
    emit_row(Record, Columns, CSV).

emit_row(Record, [C], CSV = #csv{delimiter = Delim}) ->
    [emit_cell(C, Record, CSV), Delim];
emit_row(Record, [C | Rest], CSV = #csv{separator = Sep}) ->
    [emit_cell(C, Record, CSV), Sep | emit_row(Record, Rest, CSV)];
emit_row(#{}, [], #csv{delimiter = Delim}) ->
    [Delim].

emit_cell(Column, Record, CSV) ->
    case maps:get(Column, Record, undefined) of
        undefined ->
            _Empty = "";
        Value ->
            encode_cell(emqx_template:to_string(Value), CSV)
    end.

encode_cell(V, #csv{quoting_mp = MP}) ->
    case re:run(V, MP, []) of
        nomatch ->
            V;
        _ ->
            [$", re:replace(V, <<"\"">>, <<"\"\"">>, [global, unicode]), $"]
    end.
