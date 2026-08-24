%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Restricted SQL Server INSERT INTO VALUES grammar.

Nonterminals
    template insert_stmt target static_identifier opt_static_identifier opt_columns columns identifier_list row
    expression_list expression expression_args opt_expression_args function_name opt_semicolon.

Terminals
    insert into values null default current_timestamp
    identifier placeholder number hex sq_string nq_string dq_identifier br_identifier
    '(' ')' ',' '.' ';' '+' '-' '*' '/' '%'.

Rootsymbol template.

Left 100 '+' '-'.
Left 200 '*' '/' '%'.

template -> insert_stmt : '$1'.

insert_stmt -> insert into target opt_columns values row opt_semicolon :
    {insert, #{target => '$3', columns => '$4', row => '$6'}}.

target -> static_identifier : ['$1'].
target -> static_identifier '.' static_identifier : ['$1', '$3'].
target -> static_identifier '.' opt_static_identifier '.' static_identifier : ['$1', '$3', '$5'].
target -> static_identifier '.' opt_static_identifier '.' opt_static_identifier '.' static_identifier :
    ['$1', '$3', '$5', '$7'].

static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> dq_identifier : {identifier, double, value('$1')}.
static_identifier -> br_identifier : {identifier, bracket, value('$1')}.

opt_static_identifier -> '$empty' : empty.
opt_static_identifier -> static_identifier : '$1'.

opt_columns -> '$empty' : undefined.
opt_columns -> columns : '$1'.
columns -> '(' identifier_list ')' : '$2'.
identifier_list -> static_identifier : ['$1'].
identifier_list -> identifier_list ',' static_identifier : '$1' ++ ['$3'].

row -> '(' expression_list ')' : {row, '$2'}.
expression_list -> expression : ['$1'].
expression_list -> expression_list ',' expression : '$1' ++ ['$3'].

opt_expression_args -> '$empty' : [].
opt_expression_args -> expression_args : '$1'.
expression_args -> expression : ['$1'].
expression_args -> expression_args ',' expression : '$1' ++ ['$3'].

expression -> placeholder : {var, value('$1')}.
expression -> sq_string : {string, varchar, value('$1')}.
expression -> nq_string : {string, nvarchar, value('$1')}.
expression -> number : {number, value('$1')}.
expression -> hex : {hex, value('$1')}.
expression -> null : null.
expression -> default : default.
expression -> current_timestamp : current_timestamp.
expression -> identifier : {identifier_value, value('$1')}.
expression -> function_name '(' opt_expression_args ')' : {call, '$1', '$3'}.
expression -> '(' expression ')' : {group, '$2'}.
expression -> '+' expression : {unary, '+', '$2'}.
expression -> '-' expression : {unary, '-', '$2'}.
expression -> expression '+' expression : {binary, '+', '$1', '$3'}.
expression -> expression '-' expression : {binary, '-', '$1', '$3'}.
expression -> expression '*' expression : {binary, '*', '$1', '$3'}.
expression -> expression '/' expression : {binary, '/', '$1', '$3'}.
expression -> expression '%' expression : {binary, '%', '$1', '$3'}.

function_name -> static_identifier : ['$1'].
function_name -> function_name '.' static_identifier : '$1' ++ ['$3'].

opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.

value({_Token, _Line, Value}) -> Value.
