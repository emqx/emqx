%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Restricted ClickHouse INSERT grammar for VALUES and JSONCompactEachRow templates.

Nonterminals
    template insert_stmt opt_table target static_identifier opt_columns columns column_items column_item
    identifier_list source format_name sql_rows row expression_list expression expression_args opt_expression_args
    json_array json_values opt_json_values json_value json_object json_members opt_json_members json_member
    opt_semicolon.

Terminals
    insert into table values format except null true false
    identifier placeholder number sq_string dq_string bt_identifier
    '(' ')' '[' ']' '{' '}' ',' ':' '.' ';' '+' '-' '*' '/' '%'.

Rootsymbol template.

Left 100 '+' '-'.
Left 200 '*' '/' '%'.

template -> insert_stmt : '$1'.

insert_stmt -> insert into opt_table target opt_columns source opt_semicolon :
    {insert, #{target => '$4', columns => '$5', source => '$6'}}.

opt_table -> '$empty' : false.
opt_table -> table : true.

target -> static_identifier : ['$1'].
target -> static_identifier '.' static_identifier : ['$1', '$3'].

static_identifier -> identifier : {identifier, bare, value('$1')}.
static_identifier -> dq_string : {identifier, double, value('$1')}.
static_identifier -> bt_identifier : {identifier, backtick, value('$1')}.

opt_columns -> '$empty' : undefined.
opt_columns -> columns : '$1'.

columns -> '(' column_items ')' : '$2'.
column_items -> column_item : ['$1'].
column_items -> column_items ',' column_item : '$1' ++ ['$3'].
column_item -> static_identifier : '$1'.
column_item -> '*' : star.
column_item -> '*' except '(' identifier_list ')' : {except, '$4'}.

identifier_list -> static_identifier : ['$1'].
identifier_list -> identifier_list ',' static_identifier : '$1' ++ ['$3'].

source -> values sql_rows : {values, '$2'}.
source -> format format_name sql_rows : {format_rows, '$2', '$3'}.
source -> format format_name json_array : {format_json, '$2', '$3'}.
format_name -> identifier : value('$1').
format_name -> values : <<"Values">>.

sql_rows -> row : ['$1'].
sql_rows -> sql_rows ',' row : '$1' ++ ['$3'].
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
expression -> null : null.
expression -> true : true.
expression -> false : false.
expression -> identifier : {identifier_value, value('$1')}.
expression -> identifier '(' opt_expression_args ')' : {call, value('$1'), '$3'}.
expression -> '(' expression ')' : {group, '$2'}.
expression -> '[' opt_expression_args ']' : {array, '$2'}.
expression -> '+' expression : {unary, '+', '$2'}.
expression -> '-' expression : {unary, '-', '$2'}.
expression -> expression '+' expression : {binary, '+', '$1', '$3'}.
expression -> expression '-' expression : {binary, '-', '$1', '$3'}.
expression -> expression '*' expression : {binary, '*', '$1', '$3'}.
expression -> expression '/' expression : {binary, '/', '$1', '$3'}.
expression -> expression '%' expression : {binary, '%', '$1', '$3'}.

json_array -> '[' opt_json_values ']' : {json_array, '$2'}.
opt_json_values -> '$empty' : [].
opt_json_values -> json_values : '$1'.
json_values -> json_value : ['$1'].
json_values -> json_values ',' json_value : '$1' ++ ['$3'].
json_value -> placeholder : {json_var, value('$1')}.
json_value -> dq_string : {json_string, value('$1')}.
json_value -> number : {json_number, value('$1')}.
json_value -> '-' number : {json_number, <<"-", (value('$2'))/binary>>}.
json_value -> true : true.
json_value -> false : false.
json_value -> null : null.
json_value -> json_array : '$1'.
json_value -> json_object : '$1'.

json_object -> '{' opt_json_members '}' : {json_object, '$2'}.
opt_json_members -> '$empty' : [].
opt_json_members -> json_members : '$1'.
json_members -> json_member : ['$1'].
json_members -> json_members ',' json_member : '$1' ++ ['$3'].
json_member -> dq_string ':' json_value : {value('$1'), '$3'}.

opt_semicolon -> '$empty' : false.
opt_semicolon -> ';' : true.

Erlang code.

value({_Token, _Line, Value}) -> Value.
