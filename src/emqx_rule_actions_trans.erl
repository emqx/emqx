%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_actions_trans).

-include_lib("syntax_tools/include/merl.hrl").

-export([parse_transform/2]).

parse_transform(Forms, _Options) ->
  trans(Forms, []).

trans([], ResAST) ->
  lists:reverse(ResAST);
trans([{eof, L} | AST], ResAST) ->
  lists:reverse([{eof, L} | ResAST]) ++ AST;
trans([{function, LineNo, FuncName, Arity, Clauses} | AST], ResAST) ->
  NewClauses = trans_func_clauses(atom_to_list(FuncName), Clauses),
  trans(AST, [{function, LineNo, FuncName, Arity, NewClauses} | ResAST]);
trans([Form | AST], ResAST) ->
  trans(AST, [Form | ResAST]).

trans_func_clauses("on_action_create_" ++ _ = _FuncName , Clauses) ->
  %io:format("~n[[transing function: ~p]]~n", [_FuncName]),
  %io:format("~n-----old clauses:~n", []), merl:print(Clauses),
  NewClauses = [
    begin
      Bindings = lists:flatten(get_vars(Args) ++ get_vars(Body, lefth)),
      Body2 = append_to_result(Bindings, Body),
      {clause, LineNo, Args, Guards, Body2}
    end || {clause, LineNo, Args, Guards, Body} <- Clauses],
  %io:format("~n-----new clauses: ~n"), merl:print(NewClauses),
  NewClauses;
trans_func_clauses(_FuncName, Clauses) ->
  %io:format("~n[[discarding function: ~p]]~n", [_FuncName]),
  Clauses.

get_vars(Exprs) ->
  get_vars(Exprs, all).
get_vars(Exprs, Type) ->
  do_get_vars(Exprs, [], Type).

do_get_vars([], Vars, _Type) -> Vars;
do_get_vars([Line | Expr], Vars, all) ->
  do_get_vars(Expr, [syntax_vars(erl_syntax:form_list([Line])) | Vars], all);
do_get_vars([Line | Expr], Vars, lefth) ->
  do_get_vars(Expr,
    case (Line) of
      ?Q("_@LeftV = _@@_") -> Vars ++ syntax_vars(LeftV);
      _ -> Vars
    end, lefth).

syntax_vars(Line) ->
  sets:to_list(erl_syntax_lib:variables(Line)).

%% append bindings to the return value as the first tuple element.
%% e.g. if the original result is R, then the new result will be {[binding()], R}.
append_to_result(Bindings, Exprs) ->
  erl_syntax:revert_forms(do_append_to_result(to_keyword(Bindings), Exprs, [])).

do_append_to_result(KeyWordVars, [Line], Res) ->
  case Line of
    ?Q("_@LeftV = _@RightV") ->
      lists:reverse([?Q("{[_@KeyWordVars], _@LeftV}"), Line | Res]);
    _ ->
      lists:reverse([?Q("{[_@KeyWordVars], _@Line}") | Res])
  end;
do_append_to_result(KeyWordVars, [Line | Exprs], Res) ->
  do_append_to_result(KeyWordVars, Exprs, [Line | Res]).

to_keyword(Vars) ->
  [erl_syntax:tuple([erl_syntax:atom(Var), merl:var(Var)])
   || Var <- Vars].
