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
-module(emqx_bridge_app).

-behaviour(application).

-export([start/2, stop/1]).

-export([ pre_config_update/3
        ]).

-define(TOP_LELVE_HDLR_PATH, (emqx_bridge:config_key_path())).
-define(LEAF_NODE_HDLR_PATH, (emqx_bridge:config_key_path() ++ ['?', '?'])).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_bridge_sup:start_link(),
    ok = emqx_bridge:load(),
    ok = emqx_bridge:load_hook(),
    ok = emqx_config_handler:add_handler(?LEAF_NODE_HDLR_PATH, ?MODULE),
    ok = emqx_config_handler:add_handler(?TOP_LELVE_HDLR_PATH, emqx_bridge),
    {ok, Sup}.

stop(_State) ->
    emqx_conf:remove_handler(?LEAF_NODE_HDLR_PATH),
    emqx_conf:remove_handler(?TOP_LELVE_HDLR_PATH),
    ok = emqx_bridge:unload_hook(),
    ok.

-define(IS_OPER(O), when Oper == start; Oper == stop; Oper == restart).
pre_config_update(_, {Oper, _, _}, undefined) ?IS_OPER(Oper) ->
    {error, bridge_not_found};
pre_config_update(_, {Oper, Type, Name}, OldConfig) ?IS_OPER(Oper) ->
    case perform_operation(Oper, Type, Name) of
        ok ->
            %% we also need to save the 'enable' to the config files
            {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
        {error, _} = Err -> Err
    end;
pre_config_update(_, Conf, _OldConfig) ->
    {ok, Conf}.

%% internal functions
operation_to_enable(start) -> true;
operation_to_enable(stop) -> false;
operation_to_enable(restart) -> true.

perform_operation(start, Type, Name) -> emqx_bridge:restart(Type, Name);
perform_operation(restart, Type, Name) -> emqx_bridge:restart(Type, Name);
perform_operation(stop, Type, Name) -> emqx_bridge:stop(Type, Name).
