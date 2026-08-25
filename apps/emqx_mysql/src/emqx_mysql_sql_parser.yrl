%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Restricted MySQL INSERT INTO VALUES grammar.

Nonterminals
    template insert_stmt target static_identifier name_path opt_columns columns identifier_list row
    expression_list expression named_expression opt_call expression_args opt_expression_args
    opt_alias alias alias_identifier opt_alias_columns opt_on_duplicate assignments assignment
    opt_semicolon.

Terminals
    insert into values on duplicate key update as null true false default
    identifier placeholder number hex_number hex_string sq_string dq_opaque bt_identifier
    '(' ')' ',' '.' ';' '=' '+' '-' '*' '/' '%'.

Rootsymbol template.

Left 100 '+' '-'.
Left 200 '*' '/' '%'.

template -> insert_stmt : '$1'.

insert_stmt -> insert into target opt_columns values row opt_alias opt_on_duplicate opt_semicolon :
    {insert, #{
        target => '$3',
        columns => '$4',
        row => '$6',
        alias => '$7',
        on_duplicate => '$8'
    }}.

target -> name_path : '$1'.

static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> bt_identifier : {identifier, backtick, value('$1')}.
static_identifier -> dq_opaque : {identifier, double_opaque, value('$1')}.
static_identifier -> values : {identifier, bare, value('$1')}.
static_identifier -> key : {identifier, bare, value('$1')}.
static_identifier -> update : {identifier, bare, value('$1')}.

name_path -> static_identifier : ['$1'].
name_path -> name_path '.' static_identifier : '$1' ++ ['$3'].

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
expression -> sq_string : {string, value('$1')}.
expression -> number : {number, value('$1')}.
expression -> hex_number : {hex, value('$1')}.
expression -> hex_string : {hex, value('$1')}.
expression -> null : null.
expression -> true : true.
expression -> false : false.
expression -> default : default.
expression -> named_expression : '$1'.
expression -> '(' expression ')' : {group, '$2'}.
expression -> '+' expression : {unary, '+', '$2'}.
expression -> '-' expression : {unary, '-', '$2'}.
expression -> expression '+' expression : {binary, '+', '$1', '$3'}.
expression -> expression '-' expression : {binary, '-', '$1', '$3'}.
expression -> expression '*' expression : {binary, '*', '$1', '$3'}.
expression -> expression '/' expression : {binary, '/', '$1', '$3'}.
expression -> expression '%' expression : {binary, '%', '$1', '$3'}.

named_expression -> name_path opt_call : make_named_expression('$1', '$2').
opt_call -> '$empty' : reference.
opt_call -> '(' opt_expression_args ')' : {call, '$2'}.

opt_alias -> '$empty' : undefined.
opt_alias -> alias : '$1'.
alias -> as alias_identifier opt_alias_columns : {alias, '$2', '$3'}.
alias_identifier -> identifier : {identifier, bare, value('$1')}.
alias_identifier -> bt_identifier : {identifier, backtick, value('$1')}.
alias_identifier -> dq_opaque : {identifier, double_opaque, value('$1')}.
opt_alias_columns -> '$empty' : undefined.
opt_alias_columns -> '(' identifier_list ')' : '$2'.

opt_on_duplicate -> '$empty' : undefined.
opt_on_duplicate -> on duplicate key update assignments : {on_duplicate, '$5'}.
assignments -> assignment : ['$1'].
assignments -> assignments ',' assignment : '$1' ++ ['$3'].
assignment -> name_path '=' expression : {assignment, '$1', '$3'}.

opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.

-ignore_xref({return_error, 2}).

value({_Token, _Line, Value}) -> Value.

make_named_expression(Name, reference) -> {identifier_ref, Name};
make_named_expression(Name, {call, Args}) -> {call, Name, Args}.
