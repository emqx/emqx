%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge).
-behaviour(emqx_config_handler).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([post_config_update/4]).

-export([ load_hook/0
        , reload_hook/0
        , unload_hook/0
        ]).

-export([on_message_publish/1]).

-export([ resource_type/1
        , bridge_type/1
        , resource_id/1
        , resource_id/2
        , parse_bridge_id/1
        ]).

-export([ load/0
        , lookup/2
        , lookup/3
        , list/0
        , create/3
        , remove/3
        , update/3
        , start/2
        , stop/2
        , restart/2
        ]).

-export([ send_message/2
        ]).

-export([ config_key_path/0
        ]).

reload_hook() ->
    unload_hook(),
    load_hook().

load_hook() ->
    Bridges = emqx:get_config([bridges], #{}),
    lists:foreach(fun({_Type, Bridge}) ->
            lists:foreach(fun({_Name, BridgeConf}) ->
                    load_hook(BridgeConf)
                end, maps:to_list(Bridge))
        end, maps:to_list(Bridges)).

load_hook(#{from_local_topic := _}) ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []});
load_hook(_Conf) -> ok.

unload_hook() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

on_message_publish(Message = #message{topic = Topic, flags = Flags}) ->
    case maps:get(sys, Flags, false) of
        false ->
            lists:foreach(fun (Id) ->
                    send_message(Id, emqx_rule_events:eventmsg_publish(Message))
                end, get_matched_bridges(Topic));
        true -> ok
    end,
    {ok, Message}.

send_message(BridgeId, Message) ->
    {BridgeType, BridgeName} = parse_bridge_id(BridgeId),
    ResId = emqx_bridge:resource_id(BridgeType, BridgeName),
    emqx_resource:query(ResId, {send_message, BridgeId, Message}).

config_key_path() ->
    [bridges].

resource_type(mqtt) -> emqx_connector_mqtt;
resource_type(http) -> emqx_connector_http.

bridge_type(emqx_connector_mqtt) -> mqtt;
bridge_type(emqx_connector_http) -> http.

post_config_update(_Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated}
        = diff_confs(NewConf, OldConf),
    perform_bridge_changes([
        {fun remove/3, Removed},
        {fun create/3, Added},
        {fun update/3, Updated}
    ]),
    reload_hook().

perform_bridge_changes(Tasks) ->
    perform_bridge_changes(Tasks, ok).

perform_bridge_changes([], Result) ->
    Result;
perform_bridge_changes([{Action, MapConfs} | Tasks], Result0) ->
    Result = maps:fold(fun
        ({_Type, _Name}, _Conf, {error, Reason}) ->
            {error, Reason};
        ({Type, Name}, Conf, _) ->
            case Action(Type, Name, Conf) of
                {error, Reason} -> {error, Reason};
                Return -> Return
            end
        end, Result0, MapConfs),
    perform_bridge_changes(Tasks, Result).

load() ->
    Bridges = emqx:get_config([bridges], #{}),
    emqx_bridge_monitor:ensure_all_started(Bridges).

resource_id(BridgeId) when is_binary(BridgeId) ->
    <<"bridge:", BridgeId/binary>>.

resource_id(BridgeType, BridgeName) ->
    BridgeId = bridge_id(BridgeType, BridgeName),
    resource_id(BridgeId).

bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    case string:split(bin(BridgeId), ":", all) of
        [Type, Name] -> {binary_to_atom(Type, utf8), binary_to_atom(Name, utf8)};
        _ -> error({invalid_bridge_id, BridgeId})
    end.

list() ->
    lists:foldl(fun({Type, NameAndConf}, Bridges) ->
            lists:foldl(fun({Name, RawConf}, Acc) ->
                    case lookup(Type, Name, RawConf) of
                        {error, not_found} -> Acc;
                        {ok, Res} -> [Res | Acc]
                    end
                end, Bridges, maps:to_list(NameAndConf))
        end, [], maps:to_list(emqx:get_raw_config([bridges], #{}))).

lookup(Type, Name) ->
    RawConf = emqx:get_raw_config([bridges, Type, Name], #{}),
    lookup(Type, Name, RawConf).
lookup(Type, Name, RawConf) ->
    case emqx_resource:get_instance(resource_id(Type, Name)) of
        {error, not_found} -> {error, not_found};
        {ok, Data} -> {ok, #{id => bridge_id(Type, Name), resource_data => Data,
                             raw_config => RawConf}}
    end.

start(Type, Name) ->
    restart(Type, Name).

stop(Type, Name) ->
    emqx_resource:stop(resource_id(Type, Name)).

restart(Type, Name) ->
    emqx_resource:restart(resource_id(Type, Name)).

create(Type, Name, Conf) ->
    ?SLOG(info, #{msg => "create bridge", type => Type, name => Name,
        config => Conf}),
    ResId = resource_id(Type, Name),
    case emqx_resource:create(ResId,
            emqx_bridge:resource_type(Type), parse_confs(Type, Conf)) of
        {ok, already_created} ->
            emqx_resource:get_instance(ResId);
        {ok, Data} ->
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

update(Type, Name, {_OldConf, Conf}) ->
    %% TODO: sometimes its not necessary to restart the bridge connection.
    %%
    %% - if the connection related configs like `servers` is updated, we should restart/start
    %% or stop bridges according to the change.
    %% - if the connection related configs are not update, only non-connection configs like
    %% the `method` or `headers` of a HTTP bridge is changed, then the bridge can be updated
    %% without restarting the bridge.
    %%
    ?SLOG(info, #{msg => "update bridge", type => Type, name => Name,
        config => Conf}),
    emqx_resource:recreate(resource_id(Type, Name),
        emqx_bridge:resource_type(Type), Conf, []).

remove(Type, Name, _Conf) ->
    ?SLOG(info, #{msg => "remove bridge", type => Type, name => Name}),
    case emqx_resource:remove(resource_id(Type, Name)) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} ->
            {error, Reason}
    end.

diff_confs(NewConfs, OldConfs) ->
    emqx_map_lib:diff_maps(flatten_confs(NewConfs),
        flatten_confs(OldConfs)).

flatten_confs(Conf0) ->
    maps:from_list(
        lists:flatmap(fun({Type, Conf}) ->
                do_flatten_confs(Type, Conf)
            end, maps:to_list(Conf0))).

do_flatten_confs(Type, Conf0) ->
    [{{Type, Name}, Conf} || {Name, Conf} <- maps:to_list(Conf0)].

get_matched_bridges(Topic) ->
    Bridges = emqx:get_config([bridges], #{}),
    maps:fold(fun (BType, Conf, Acc0) ->
        maps:fold(fun
            (BName, #{egress := Egress}, Acc1) ->
                get_matched_bridge_id(Egress, Topic, BType, BName, Acc1);
            (BName, BridgeConf, Acc1) ->
                get_matched_bridge_id(BridgeConf, Topic, BType, BName, Acc1)
        end, Acc0, Conf)
    end, [], Bridges).

get_matched_bridge_id(#{from_local_topic := Filter}, Topic, BType, BName, Acc) ->
    case emqx_topic:match(Topic, Filter) of
        true -> [bridge_id(BType, BName) | Acc];
        false -> Acc
    end.

parse_confs(http, #{ url := Url
                   , method := Method
                   , body := Body
                   , headers := Headers
                   , request_timeout := ReqTimeout
                   } = Conf) ->
    {BaseUrl, Path} = parse_url(Url),
    {ok, BaseUrl2} = emqx_http_lib:uri_parse(BaseUrl),
    Conf#{ base_url => BaseUrl2
         , request =>
            #{ path => Path
             , method => Method
             , body => Body
             , headers => Headers
             , request_timeout => ReqTimeout
             }
         };
parse_confs(_Type, Conf) ->
    Conf.

parse_url(Url) ->
    case string:split(Url, "//", leading) of
        [Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [HostPort, Path] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), Path};
                [HostPort] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), <<>>}
            end;
        [Url] ->
            error({invalid_url, Url})
    end.


bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
