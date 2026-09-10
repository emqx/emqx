%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Restricted Doris INSERT VALUES grammar, not a full dialect parser.
%% Source: supportedDmlStatement, inlineTable, rowConstructor, expression,
%% primaryExpression, multipartIdentifier in Doris 2.1.9:
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4
%% DEFAULT is a direct row item only. Row aliases and duplicate-key suffixes
%% are absent. One template row expands to multiple rows during rendering.

Nonterminals
    template name_path static_identifier opt_columns identifier_list row_items row_item
    expression unary_expression primary args opt_args case_expression opt_operand whens opt_else opt_semicolon.
Terminals
    insert into values null true false default
    builtin_expression
    case_kw when_kw then_kw else_kw end_kw is_kw and_kw or_kw not_kw
    identifier placeholder number string bt_identifier
    '(' ')' ',' '.' ';' '=' '<=>' '>=' '<=' '<>' '>' '<' '+' '-' '*' '/' '%'.
Rootsymbol template.
Left 10 or_kw.
Left 20 and_kw.
Right 30 not_kw.
Nonassoc 40 '=' '<=>' '>=' '<=' '<>' '>' '<' is_kw.
Left 100 '+' '-'.
Left 200 '*' '/' '%'.

template -> insert into name_path opt_columns values '(' row_items ')' opt_semicolon :
    {insert, '$3', '$4', '$7'}.
static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> bt_identifier : {identifier, backtick, value('$1')}.
name_path -> static_identifier : ['$1'].
name_path -> name_path '.' static_identifier : '$1' ++ ['$3'].
opt_columns -> '$empty' : undefined.
opt_columns -> '(' identifier_list ')' : '$2'.
identifier_list -> static_identifier : ['$1'].
identifier_list -> identifier_list ',' static_identifier : '$1' ++ ['$3'].
row_items -> row_item : ['$1'].
row_items -> row_items ',' row_item : '$1' ++ ['$3'].
row_item -> default : default.
row_item -> expression : '$1'.
opt_args -> '$empty' : [].
opt_args -> args : '$1'.
args -> expression : ['$1'].
args -> args ',' expression : '$1' ++ ['$3'].
primary -> placeholder : {var, value('$1')}.
primary -> string : {string, value('$1')}.
primary -> number : {number, value('$1')}.
primary -> null : null.
primary -> true : true.
primary -> false : false.
%% Bare temporal and user expressions from primaryExpression.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4#L1479-L1485
primary -> builtin_expression : {builtin_expression, value('$1')}.
primary -> name_path : {identifier_ref, '$1'}.
primary -> name_path '(' opt_args ')' : {call, '$1', '$3'}.
primary -> '(' expression ')' : {group, '$2'}.
primary -> case_expression : '$1'.
unary_expression -> primary : '$1'.
unary_expression -> '+' unary_expression : {unary, '+', '$2'}.
unary_expression -> '-' unary_expression : {unary, '-', '$2'}.
expression -> unary_expression : '$1'.
expression -> not_kw expression : {unary, 'NOT', '$2'}.
expression -> expression '=' expression : {binary, '=', '$1', '$3'}.
expression -> expression '<=>' expression : {binary, '<=>', '$1', '$3'}.
expression -> expression '>=' expression : {binary, '>=', '$1', '$3'}.
expression -> expression '<=' expression : {binary, '<=', '$1', '$3'}.
expression -> expression '<>' expression : {binary, '<>', '$1', '$3'}.
expression -> expression '>' expression : {binary, '>', '$1', '$3'}.
expression -> expression '<' expression : {binary, '<', '$1', '$3'}.
expression -> expression is_kw null : {is_null, '$1', false}.
expression -> expression is_kw not_kw null : {is_null, '$1', true}.
expression -> expression and_kw expression : {binary, 'AND', '$1', '$3'}.
expression -> expression or_kw expression : {binary, 'OR', '$1', '$3'}.
expression -> expression '+' expression : {binary, '+', '$1', '$3'}.
expression -> expression '-' expression : {binary, '-', '$1', '$3'}.
expression -> expression '*' expression : {binary, '*', '$1', '$3'}.
expression -> expression '/' expression : {binary, '/', '$1', '$3'}.
expression -> expression '%' expression : {binary, '%', '$1', '$3'}.
case_expression -> case_kw opt_operand whens opt_else end_kw :
    {case_expression, '$2', '$3', '$4'}.
opt_operand -> '$empty' : undefined.
opt_operand -> expression : '$1'.
whens -> when_kw expression then_kw expression : [{'when', '$2', '$4'}].
whens -> whens when_kw expression then_kw expression : '$1' ++ [{'when', '$3', '$5'}].
opt_else -> '$empty' : undefined.
opt_else -> else_kw expression : '$2'.
opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.
-ignore_xref({return_error, 2}).
value({_Token, _Line, Value}) -> Value.
