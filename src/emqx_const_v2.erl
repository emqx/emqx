%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc Never update this module, create a v3 instead.
%%--------------------------------------------------------------------

-module(emqx_const_v2).

-export([ make_tls_root_fun/2
        ]).

make_tls_root_fun(cacert_from_cacertfile, CADer) ->
    fun(InputChain) ->
            case lists:member(CADer, InputChain) of
                true -> {trusted_ca, CADer};
                _ -> unknown_ca
            end
    end.
