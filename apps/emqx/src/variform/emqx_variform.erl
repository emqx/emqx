%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Predefined functions for templating
-module(emqx_variform).

-export([render/2]).

render(Expression, Context) ->
    case emqx_variform_scan:string(Expression) of
        {ok, Tokens, _Line} ->
            case emqx_variform_parser:parse(Tokens) of
                {ok, Expr} ->
                    eval(Expr, Context);
                {error, {_, emqx_variform_parser, Msg}} ->
                    %% syntax error
                    {error, lists:flatten(Msg)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason, _Line} ->
            {error, Reason}
    end.

eval(Expr, _Context) ->
    io:format(user, "~p~n", [Expr]).
