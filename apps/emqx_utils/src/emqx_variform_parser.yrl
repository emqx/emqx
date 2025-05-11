Nonterminals
    expr
    call_or_var
    array
    args
    arg.

Terminals
    identifier
    integer
    float
    string
    boolean
    '(' ')'
    ',' '[' ']'.

Rootsymbol
    expr.

%% Grammar Rules

%% Root expression: function call or variable or a boolean
expr -> call_or_var : '$1'.
expr -> boolean: element(3, '$1').
expr -> integer: {integer, element(3, '$1')}.
expr -> float: {float, element(3, '$1')}.
expr -> string : {str, element(3, '$1')}.

%% Function call or variable
call_or_var -> identifier '(' ')' : {call, element(3, '$1'), []}.
call_or_var -> identifier '(' args ')' : {call, element(3, '$1'), '$3'}.
call_or_var -> identifier : {var, element(3, '$1')}.

%% Array is like a arg list, but with square brackets
array -> '[' args ']' : {array, '$2'}.

%% Argument handling
args -> arg : ['$1'].
args -> args ',' arg : '$1' ++ ['$3'].

%% Arguments can be expressions, arrays, numbers, strings or booleans
arg -> expr : '$1'.
arg -> array : '$1'.

Erlang code.

%% mute xref warning
-export([return_error/2]).
