%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Restricted QuasarDB 3.14.1 INSERT VALUES grammar.
%% The published grammar permits several rows. One template row expands to
%% several rows during batch rendering:
%% https://doc.quasar.ai/3.14.1/queries/insert.html#synopsis
%% Values are restricted to the atomic forms accepted by INSERT in 3.14.1.
%% General expressions documented for SELECT are not accepted by INSERT:
%% https://doc.quasar.ai/3.14.1/queries/select.html#synopsis

Nonterminals
    template static_identifier identifier_list row value opt_semicolon.
Terminals
    insert into values null
    identifier dq_identifier timestamp_column placeholder sq_string number timestamp
    relative_timestamp timestamp_constant
    '(' ')' ',' ';'.
Rootsymbol template.

template -> insert into static_identifier '(' identifier_list ')' values '(' row ')' opt_semicolon :
    {insert, '$3', '$5', '$9'}.

static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> dq_identifier : {identifier, double, value('$1')}.
static_identifier -> timestamp_column : {identifier, special, value('$1')}.
static_identifier -> insert : {identifier, bare, value('$1')}.
static_identifier -> into : {identifier, bare, value('$1')}.
static_identifier -> values : {identifier, bare, value('$1')}.
static_identifier -> null : {identifier, bare, value('$1')}.
static_identifier -> relative_timestamp : {identifier, bare, value('$1')}.
static_identifier -> timestamp_constant : {identifier, bare, value('$1')}.

identifier_list -> static_identifier : ['$1'].
identifier_list -> identifier_list ',' static_identifier : '$1' ++ ['$3'].

row -> value : ['$1'].
row -> row ',' value : '$1' ++ ['$3'].

value -> placeholder : {var, value('$1')}.
value -> sq_string : {string, value('$1')}.
value -> number : {number, value('$1')}.
value -> timestamp : {timestamp, value('$1')}.
value -> null : null.
value -> relative_timestamp : {timestamp_name, value('$1'), bare}.
value -> relative_timestamp '(' ')' : {timestamp_name, value('$1'), call}.
value -> timestamp_constant : {timestamp_name, value('$1'), bare}.
value -> timestamp_constant '(' ')' : {timestamp_name, value('$1'), call}.

opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.
-ignore_xref({return_error, 2}).
value({_Token, _Line, Value}) -> Value.
