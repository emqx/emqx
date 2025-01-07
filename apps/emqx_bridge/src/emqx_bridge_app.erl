%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-export([start/2, stop/1]).

-export([
    pre_config_update/3,
    post_config_update/5
]).

-define(TOP_LELVE_HDLR_PATH, (emqx_bridge:config_key_path())).
-define(LEAF_NODE_HDLR_PATH, (emqx_bridge:config_key_path() ++ ['?', '?'])).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_bridge_sup:start_link(),
    ok = ensure_enterprise_schema_loaded(),
    ok = emqx_bridge:load(),
    ok = emqx_bridge_v2:load(),
    ok = emqx_bridge:load_hook(),
    ok = emqx_config_handler:add_handler(?LEAF_NODE_HDLR_PATH, ?MODULE),
    ok = emqx_config_handler:add_handler(?TOP_LELVE_HDLR_PATH, emqx_bridge),
    ?tp(emqx_bridge_app_started, #{}),
    {ok, Sup}.

stop(_State) ->
    emqx_conf:remove_handler(?LEAF_NODE_HDLR_PATH),
    emqx_conf:remove_handler(?TOP_LELVE_HDLR_PATH),
    ok = emqx_bridge:unload(),
    ok = emqx_bridge_v2:unload(),
    emqx_action_info:clean_cache(),
    ok.

-if(?EMQX_RELEASE_EDITION == ee).
ensure_enterprise_schema_loaded() ->
    emqx_utils:interactive_load(emqx_bridge_enterprise),
    ok.
-else.
ensure_enterprise_schema_loaded() ->
    ok.
-endif.

%% NOTE: We depends on the `emqx_bridge:pre_config_update/3` to restart/stop the
%%       underlying resources.
pre_config_update(_, {_Oper, _Type, _Name}, undefined) ->
    {error, bridge_not_found};
pre_config_update(_, {Oper, _Type, _Name}, OldConfig) ->
    %% to save the 'enable' to the config files
    {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update(Path, Conf, _OldConfig) when is_map(Conf) ->
    case validate_bridge_name_in_config(Path) of
        ok ->
            case emqx_connector_ssl:convert_certs(filename:join(Path), Conf) of
                {error, Reason} ->
                    {error, Reason};
                {ok, ConfNew} ->
                    {ok, ConfNew}
            end;
        Error ->
            Error
    end.

post_config_update([bridges, BridgeType, BridgeName], '$remove', _, _OldConf, _AppEnvs) ->
    ok = emqx_bridge_resource:remove(BridgeType, BridgeName),
    Bridges = emqx_utils_maps:deep_remove([BridgeType, BridgeName], emqx:get_config([bridges])),
    emqx_bridge:reload_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([bridges, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs) ->
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_bridge_resource:create(BridgeType, BridgeName, NewConf, ResOpts),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([bridges]), NewConf
    ),
    emqx_bridge:reload_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([bridges, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs) ->
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_bridge_resource:update(BridgeType, BridgeName, {OldConf, NewConf}, ResOpts),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([bridges]), NewConf
    ),
    emqx_bridge:reload_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok.

%% internal functions
operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

validate_bridge_name_in_config(Path) ->
    [RootKey] = emqx_bridge:config_key_path(),
    case Path of
        [RootKey, _BridgeType, BridgeName] ->
            validate_bridge_name(BridgeName);
        _ ->
            ok
    end.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(B) when is_binary(B) -> B.

validate_bridge_name(BridgeName) ->
    try
        _ = emqx_resource:validate_name(to_bin(BridgeName)),
        ok
    catch
        throw:Error ->
            {error, Error}
    end.
