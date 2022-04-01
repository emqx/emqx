%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Simplified semi-automatic CPS mode tree for coap
%% The tree must have a terminal leaf node, and it's return is the result of the entire tree.
%% This module currently only supports simple linear operation

-module(emqx_coap_medium).

-include("src/coap/include/emqx_coap.hrl").

%% API
-export([
    empty/0,
    reset/1, reset/2,
    out/1, out/2,
    proto_out/1,
    proto_out/2,
    iter/3, iter/4,
    reply/2, reply/3, reply/4
]).

%%-type result() :: map() | empty.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
empty() -> #{}.

reset(Msg) ->
    reset(Msg, #{}).

reset(Msg, Result) ->
    out(emqx_coap_message:reset(Msg), Result).

out(Msg) ->
    #{out => [Msg]}.

out(Msg, #{out := Outs} = Result) ->
    Result#{out := [Msg | Outs]};
out(Msg, Result) ->
    Result#{out => [Msg]}.

proto_out(Proto) ->
    proto_out(Proto, #{}).

proto_out(Proto, Result) ->
    Result#{proto => Proto}.

reply(Method, Req) when not is_record(Method, coap_message) ->
    reply(Method, <<>>, Req);
reply(Reply, Result) ->
    Result#{reply => Reply}.

reply(Method, Req, Result) when is_record(Req, coap_message) ->
    reply(Method, <<>>, Req, Result);
reply(Method, Payload, Req) ->
    reply(Method, Payload, Req, #{}).

reply(Method, Payload, Req, Result) ->
    Result#{reply => emqx_coap_message:piggyback(Method, Payload, Req)}.

%% run a tree
iter([Key, Fun | T], Input, State) ->
    case maps:get(Key, Input, undefined) of
        undefined ->
            iter(T, Input, State);
        Val ->
            Fun(Val, maps:remove(Key, Input), State, T)
        %% reserved
        %% if is_function(Fun) ->
        %%         Fun(Val, maps:remove(Key, Input), State, T);
        %%    true ->
        %%         %% switch to sub branch
        %%         [FunH | FunT] = Fun,
        %%         FunH(Val, maps:remove(Key, Input), State, FunT)
        %% end
    end;
%% terminal node
iter([Fun], Input, State) ->
    Fun(undefined, Input, State).

%% run a tree with argument
iter([Key, Fun | T], Input, Arg, State) ->
    case maps:get(Key, Input, undefined) of
        undefined ->
            iter(T, Input, Arg, State);
        Val ->
            Fun(Val, maps:remove(Key, Input), Arg, State, T)
    end;
iter([Fun], Input, Arg, State) ->
    Fun(undefined, Input, Arg, State).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
