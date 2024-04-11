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
    '(' ')'
    ',' '[' ']'.

Rootsymbol
    expr.

%% Grammar Rules

%% Root expression: function call or variable
expr -> call_or_var : '$1'.

%% Function call or variable
call_or_var -> identifier '(' args ')' : {call, element(3, '$1'), '$3'}.
call_or_var -> identifier : {var, element(3, '$1')}.

%% Array is like a arg list, but with square brackets
array -> '[' args ']' : {array, '$2'}.

%% Argument handling
args -> arg : ['$1'].
args -> args ',' arg : '$1' ++ ['$3'].

%% Arguments can be expressions, arrays, numbers, or strings
arg -> expr : '$1'.
arg -> array : '$1'.
arg -> integer: {integer, element(3, '$1')}.
arg -> float: {float, element(3, '$1')}.
arg -> string : {str, element(3, '$1')}.

Erlang code.

%% mute xref warning
-export([return_error/2]).
