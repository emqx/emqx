%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module provides a parse_transform to inject emqx version number
%% to all modules as a module attribute.
%% The module attribute is so far only used for beam reload inspection.
-module(emqx_mod_vsn).

-export([parse_transform/2]).

parse_transform(Form, _Opts) ->
    case os:getenv("PKG_VSN") of
        false -> Form;
        Vsn -> trans(Form, {attribute, 1, emqx_vsn, Vsn})
    end.

trans(Form, Injection) ->
    trans(Form, Injection, []).

trans([], _Injection, Acc) ->
    lists:reverse(Acc);
trans([{eof, _} | _] = EOF, _Injection, Acc) ->
    lists:reverse(Acc) ++ EOF;
trans([{attribute, _, module, _} = Module | Form], Injection, Acc) ->
    lists:reverse(Acc) ++ [Module, Injection | Form];
trans([H | T], Injection, Acc) ->
    trans(T, Injection, [H | Acc]).
