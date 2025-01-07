%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc File Transfer error description module

-module(emqx_ft_error).

-export([format/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format(ok) -> <<"success">>;
format({error, Reason}) -> format_error_reson(Reason).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

format_error_reson(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
format_error_reson({ErrorKind, _}) when is_atom(ErrorKind) ->
    atom_to_binary(ErrorKind, utf8);
format_error_reson(_Reason) ->
    <<"internal_error">>.
