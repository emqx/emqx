%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_authn).
-define(CHAIN, <<"mqtt">>).

-type chain_id() :: binary().
-type authenticator_name() :: binary().
-type mechanism() :: 'password-based' | jwt | scram.

-record(authenticator,
        { name :: authenticator_name()
        , mechanism :: mechanism()
        , provider :: module()
        , config :: map()
        , state :: map()
        }).

-record(chain,
        { id :: chain_id()
        , authenticators :: [{authenticator_name(), #authenticator{}}]
        , created_at :: integer()
        }).

-define(AUTH_SHARD, emqx_authn_shard).

-define(CLUSTER_CALL(Module, Func, Args), ?CLUSTER_CALL(Module, Func, Args, ok)).

-define(CLUSTER_CALL(Module, Func, Args, ResParttern),
    fun() ->
        case LocalResult = erlang:apply(Module, Func, Args) of
            ResParttern ->
                Nodes = nodes(),
                {ResL, BadNodes} = rpc:multicall(Nodes, Module, Func, Args, 5000),
                NResL = lists:zip(Nodes - BadNodes, ResL),
                Errors = lists:filter(fun({_, ResParttern}) -> false;
                                         (_) -> true
                                      end, NResL),
                OtherErrors = [{BadNode, node_does_not_exist} || BadNode <- BadNodes],
                case Errors ++ OtherErrors of
                    [] -> LocalResult;
                    NErrors -> {error, NErrors}
                end;
            ErrorResult ->
                {error, ErrorResult}
        end
    end()).
