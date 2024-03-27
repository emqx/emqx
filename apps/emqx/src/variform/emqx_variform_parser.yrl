Nonterminals expr call_or_var args.
Terminals identifier number string '(' ')' ','.

Rootsymbol expr.

%% Grammar Rules
expr -> call_or_var: '$1'.

call_or_var -> identifier '(' args ')' : {call, element(3,'$1'), '$3'}.
call_or_var -> identifier : {var, element(3, '$1')}.
args -> expr : ['$1'].
args -> args ',' expr : '$1' ++ ['$3'].

%% Handling direct values and variables within arguments
expr -> number : {num, element(3, '$1')}.
expr -> string : {str, element(3, '$1')}.
