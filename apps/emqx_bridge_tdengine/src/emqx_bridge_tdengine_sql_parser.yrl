%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Restricted TDengine multi-table INSERT grammar.

Nonterminals
    template insert_stmt table_clauses table_clause target target_component static_identifier static_qualified_identifier
    opt_columns columns identifier_list using_clause opt_tag_columns opt_post_columns
    value_list row values_list value time_base opt_call duration_ops
    opt_semicolon.

Terminals
    insert into using tags values null true false now today
    identifier identifier_template bt_identifier placeholder number duration sq_string dq_string
    '(' ')' ',' '.' ';' '+' '-'.

%% Trailing unconsumed tokens do not match and cause a parse error.
Rootsymbol template.

template -> insert_stmt : '$1'.

%% This grammar is a restricted subset of TDengine multi-table INSERT:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parInsertSql.c#L1866-L1877
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parInsertSql.c#L929-L1049
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parInsertSql.c#L1508-L1651
insert_stmt -> insert into table_clauses opt_semicolon :
    {insert, #{clauses => lists:reverse('$3')}}.

table_clauses -> table_clause : ['$1'].
table_clauses -> table_clauses table_clause : ['$2' | '$1'].
table_clauses -> table_clauses ',' table_clause : ['$3' | '$1'].
table_clauses -> table_clauses row : add_row('$1', '$2').
table_clauses -> table_clauses ',' row : add_row('$1', '$3').

table_clause -> target opt_columns values row :
    {table_clause, #{
        target => '$1',
        columns_before_using => '$2',
        using => undefined,
        columns_after_using => undefined,
        rows => ['$4']
    }}.
table_clause -> target opt_columns using_clause opt_post_columns values row :
    {table_clause, #{
        target => '$1',
        columns_before_using => '$2',
        using => '$3',
        columns_after_using => '$4',
        rows => ['$6']
    }}.

target -> target_component : {target, undefined, '$1'}.
target -> static_identifier '.' target_component : {target, '$1', '$3'}.

target_component -> static_identifier : '$1'.
target_component -> identifier_template : {identifier_parts, value('$1')}.
target_component -> placeholder : {identifier_placeholder, value('$1')}.

static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> bt_identifier : {identifier_parts, value('$1')}.
static_qualified_identifier -> static_identifier : ['$1'].
static_qualified_identifier -> static_identifier '.' static_identifier : ['$1', '$3'].

opt_columns -> '$empty' : undefined.
opt_columns -> columns : '$1'.
columns -> '(' identifier_list ')' : '$2'.
identifier_list -> static_identifier : ['$1'].
identifier_list -> identifier_list ',' static_identifier : '$1' ++ ['$3'].

using_clause -> using static_qualified_identifier opt_tag_columns tags value_list :
    #{stable => '$2', tag_columns => '$3', tags => '$5'}.

opt_tag_columns -> '$empty' : undefined.
opt_tag_columns -> columns : '$1'.
opt_post_columns -> '$empty' : undefined.
opt_post_columns -> columns : '$1'.

value_list -> '(' values_list ')' : '$2'.
row -> '(' values_list ')' : {row, '$2'}.
values_list -> value : ['$1'].
values_list -> values_list ',' value : '$1' ++ ['$3'].

value -> time_base : '$1'.
%% TDengine's INSERT parser reads one signed duration after a time base:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parInsertSql.c#L300-L329
value -> time_base duration_ops : apply_duration_ops('$1', '$2').
time_base -> placeholder : {var, value('$1')}.
time_base -> sq_string : {string, single, value('$1')}.
time_base -> dq_string : {string, double, value('$1')}.
time_base -> number : {number, value('$1')}.
time_base -> '+' number : {number, <<"+", (value('$2'))/binary>>}.
time_base -> '-' number : {number, <<"-", (value('$2'))/binary>>}.
time_base -> null : null.
time_base -> true : true.
time_base -> false : false.
time_base -> now opt_call : now.
time_base -> today opt_call : today.

opt_call -> '$empty' : false.
opt_call -> '(' ')' : true.
duration_ops -> '+' duration : [{'+', value('$2')}].
duration_ops -> '-' duration : [{'-', value('$2')}].
duration_ops -> duration_ops '+' duration : '$1' ++ [{'+', value('$3')}].
duration_ops -> duration_ops '-' duration : '$1' ++ [{'-', value('$3')}].

opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.

-ignore_xref({return_error, 2}).

value({_Token, _Line, Value}) -> Value.

apply_duration_ops(Base, Ops) -> {time_arithmetic, Base, Ops}.

add_row([{table_clause, #{rows := Rows} = Clause} | Rest], Row) ->
    [{table_clause, Clause#{rows := Rows ++ [Row]}} | Rest].
