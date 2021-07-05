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
%% The sub config handlers maintain independent parts of the emqx config map
%% And there are a top level config handler maintains the overall config map.
-module(emqx_config_handler).

-include("logger.hrl").

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , add_handler/2
        , update_config/3
        , merge_to_old_config/2
        ]).

%% emqx_config_handler callbacks
-export([ handle_update_config/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MOD, {mod}).

-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([handle_update_config/2]).

-callback handle_update_config(emqx_config:update_request(), emqx_config:raw_config()) ->
    emqx_config:update_request().

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(emqx_config:config_key_path(), emqx_config:update_request(),
        emqx_config:raw_config()) ->
    ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq, RawConfig) ->
    gen_server:call(?MODULE, {update_config, ConfKeyPath, UpdateReq, RawConfig}).

-spec add_handler(emqx_config:config_key_path(), handler_name()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    gen_server:call(?MODULE, {add_child, ConfKeyPath, HandlerName}).

%%============================================================================

-spec init(term()) -> {ok, state()}.
init(_) ->
    {ok, RawConf} = hocon:load(emqx_conf_name(), #{format => richmap}),
    {_MappedEnvs, Conf} = hocon_schema:map_translate(emqx_schema, RawConf, #{}),
    ok = save_config_to_emqx(to_plainmap(Conf), to_plainmap(RawConf)),
    {ok, #{handlers => #{?MOD => ?MODULE}}}.
handle_call({add_child, ConfKeyPath, HandlerName}, _From,
            State = #{handlers := Handlers}) ->
    {reply, ok, State#{handlers =>
        emqx_map_lib:deep_put(ConfKeyPath, Handlers, #{?MOD => HandlerName})}};

handle_call({update_config, ConfKeyPath, UpdateReq, RawConf}, _From,
            #{handlers := Handlers} = State) ->
    try {RootKeys, Conf} = do_update_config(ConfKeyPath, Handlers, RawConf, UpdateReq),
        {reply, save_configs(RootKeys, Conf), State}
    catch
        throw: Reason ->
            {reply, {error, Reason}, State};
        Error : Reason : ST ->
            ?LOG(error, "update config failed: ~p", [{Error, Reason, ST}]),
            {reply, {error, Reason}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_update_config([], Handlers, OldConf, UpdateReq) ->
    call_handle_update_config(Handlers, OldConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers, OldConf, UpdateReq) ->
    SubOldConf = get_sub_config(ConfKey, OldConf),
    case maps:find(ConfKey, Handlers) of
        error -> throw({handler_not_found, ConfKey});
        {ok, SubHandlers} ->
            NewUpdateReq = do_update_config(ConfKeyPath, SubHandlers, SubOldConf, UpdateReq),
            call_handle_update_config(Handlers, OldConf, #{bin(ConfKey) => NewUpdateReq})
    end.

get_sub_config(_, undefined) ->
    undefined;
get_sub_config(ConfKey, OldConf) when is_map(OldConf) ->
    maps:get(bin(ConfKey), OldConf, undefined);
get_sub_config(_, OldConf) ->
    OldConf.

call_handle_update_config(Handlers, OldConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(UpdateReq, OldConf);
        false -> UpdateReq %% the default behaviour is overwriting the old config
    end.

%% callbacks for the top-level handler
handle_update_config(UpdateReq, OldConf) ->
    FullRawConf = merge_to_old_config(UpdateReq, OldConf),
    {maps:keys(UpdateReq), FullRawConf}.

%% default callback of config handlers
merge_to_old_config(UpdateReq, undefined) ->
    merge_to_old_config(UpdateReq, #{});
merge_to_old_config(UpdateReq, RawConf) ->
    maps:merge(RawConf, UpdateReq).

%%============================================================================
save_configs(RootKeys, RawConf) ->
    {_MappedEnvs, Conf} = hocon_schema:map_translate(emqx_schema, to_richmap(RawConf), #{}),
    %% We may need also support hot config update for the apps that use application envs.
    %% If so uncomment the following line to update the configs to application env
    %save_config_to_app_env(_MappedEnvs),
    save_config_to_emqx(to_plainmap(Conf), RawConf),
    save_config_to_disk(RootKeys, RawConf).

% save_config_to_app_env(MappedEnvs) ->
%     lists:foreach(fun({AppName, Envs}) ->
%             [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
%         end, MappedEnvs).

save_config_to_emqx(Conf, RawConf) ->
    ?LOG(debug, "set config: ~p", [Conf]),
    emqx_config:put(emqx_map_lib:unsafe_atom_key_map(Conf)),
    emqx_config:put_raw(RawConf).

save_config_to_disk(RootKeys, Conf) ->
    FileName = emqx_override_conf_name(),
    OldConf = read_old_config(FileName),
    %% We don't save the overall config to file, but only the sub configs
    %% under RootKeys
    write_new_config(FileName,
        maps:merge(OldConf, maps:with(RootKeys, Conf))).

write_new_config(FileName, Conf) ->
    case file:write_file(FileName, jsx:prettify(jsx:encode(Conf))) of
        ok -> ok;
        {error, Reason} ->
            logger:error("write to ~s failed, ~p", [FileName, Reason]),
            {error, Reason}
    end.

read_old_config(FileName) ->
    case file:read_file(FileName) of
        {ok, Text} ->
            try jsx:decode(Text, [{return_maps, true}]) of
                Conf when is_map(Conf) -> Conf;
                _ -> #{}
            catch _Err : _Reason ->
                #{}
            end;
        _ -> #{}
    end.

emqx_conf_name() ->
    filename:join([etc_dir(), "emqx.conf"]).

emqx_override_conf_name() ->
    filename:join([emqx:get_env(data_dir), "emqx_override.conf"]).

etc_dir() ->
    emqx:get_env(etc_dir).

to_richmap(Map) ->
    {ok, RichMap} = hocon:binary(jsx:encode(Map), #{format => richmap}),
    RichMap.

to_plainmap(RichMap) ->
    hocon_schema:richmap_to_map(RichMap).

bin(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).
