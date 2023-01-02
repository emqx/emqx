%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_data_backup).

-include("emqx_mgmt.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("kernel/include/file.hrl").

-ifdef(EMQX_ENTERPRISE).
-export([ export_modules/0
        , export_schemas/0
        , export_confs/0
        , import_modules/1
        , import_schemas/1
        , import_confs/2
        ]).
-endif.

-define(BACKUP_DIR, backup).

-export([ export_rules/0
        , export_resources/0
        , export_blacklist/0
        , export_applications/0
        , export_users/0
        , export_auth_mnesia/0
        , export_acl_mnesia/0
        , import_resources_and_rules/3
        , import_rules/1
        , import_resources/1
        , import_blacklist/1
        , import_applications/1
        , import_users/1
        , import_auth_clientid/1 %% BACKW: 4.1.x
        , import_auth_username/1 %% BACKW: 4.1.x
        , import_auth_mnesia/1
        , import_acl_mnesia/1
        , to_version/1
        ]).

-export([ export/0
        , import/2
        , upload_backup_file/2
        , list_backup_file/0
        , read_backup_file/1
        , delete_backup_file/1
        ]).

-ifdef(TEST).
-export([ backup_dir/0
        , delete_all_backup_file/0
        ]).
-endif.

%%--------------------------------------------------------------------
%% Data Export and Import
%%--------------------------------------------------------------------

export_rules() ->
    lists:map(fun(#rule{id = RuleId,
                        rawsql = RawSQL,
                        actions = Actions,
                        enabled = Enabled,
                        description = Desc}) ->
                   [{id, RuleId},
                    {rawsql, RawSQL},
                    {actions, actions_to_prop_list(Actions)},
                    {enabled, Enabled},
                    {description, Desc}]
               end, emqx_rule_registry:get_rules()).

export_resources() ->
    lists:map(fun(#resource{id = Id,
                            type = Type,
                            config = Config,
                            created_at = CreatedAt,
                            description = Desc}) ->
                   NCreatedAt = case CreatedAt of
                                    undefined -> null;
                                    _ -> CreatedAt
                                end,
                   [{id, Id},
                    {type, Type},
                    {config, maps:to_list(Config)},
                    {created_at, NCreatedAt},
                    {description, Desc}]
               end, emqx_rule_registry:get_resources()).

export_blacklist() ->
    lists:map(fun(#banned{who = Who, by = By, reason = Reason, at = At, until = Until}) ->
                  NWho = case Who of
                             {peerhost, Peerhost} -> {peerhost, inet:ntoa(Peerhost)};
                             _ -> Who
                         end,
                  [{who, [NWho]}, {by, By}, {reason, Reason}, {at, At}, {until, Until}]
              end, ets:tab2list(emqx_banned)).

export_applications() ->
    lists:map(fun({_, AppID, AppSecret, Name, Desc, Status, Expired}) ->
                  [{id, AppID}, {secret, AppSecret}, {name, Name}, {desc, Desc}, {status, Status}, {expired, Expired}]
              end, ets:tab2list(mqtt_app)).

export_users() ->
    lists:map(fun({_, Username, Password, Tags}) ->
                  [{username, Username}, {password, base64:encode(Password)}, {tags, Tags}]
              end, ets:tab2list(mqtt_admin)).

export_auth_mnesia() ->
    case ets:info(emqx_user) of
        undefined -> [];
        _ ->
            lists:map(fun({_, {Type, Login}, Password, CreatedAt}) ->
                          [{login, Login}, {type, Type}, {password, base64:encode(Password)}, {created_at, CreatedAt}]
                      end, ets:tab2list(emqx_user))
    end.

export_acl_mnesia() ->
    case ets:info(emqx_acl2) of
        undefined -> [];
        _ ->
            lists:map(fun({Login, Topic, Action, Access, CreatedAt}) ->
                          Filter1 = case Login of
                              {Type, TypeValue} ->
                                  [{type, Type}, {type_value, TypeValue}, {topic, Topic}];
                              Type ->
                                  [{type, Type}, {topic, Topic}]
                          end,
                          Filter1 ++ [{action, Action}, {access, Access}, {created_at, CreatedAt}]
                      end, emqx_acl_mnesia_db:all_acls_export())
    end.

-ifdef(EMQX_ENTERPRISE).
export_modules() ->
    case ets:info(emqx_modules) of
        undefined -> [];
        _ ->
           lists:map(fun({_, Id, Type, Config, Enabled, CreatedAt, Description}) ->
                          [{id, Id},
                           {type, Type},
                           {config, Config},
                           {enabled, Enabled},
                           {created_at, CreatedAt},
                           {description, Description}
                          ]
                      end, ets:tab2list(emqx_modules))
    end.

export_schemas() ->
    case ets:info(emqx_schema) of
        undefined -> [];
        _ ->
            [emqx_schema_api:format_schema(Schema) || Schema <- emqx_schema_registry:get_all_schemas()]
    end.

export_confs() ->
    case ets:info(emqx_conf_info) of
        undefined -> {[], []};
        _ ->
            {lists:map(fun({_, Key, Confs}) ->
                case Key of
                    {_Zone, Name} ->
                        [{zone, iolist_to_binary(Name)},
                         {confs, confs_to_binary(Confs)}];
                    {_Listener, Type, Name} ->
                        [{type, iolist_to_binary(Type)},
                         {name, iolist_to_binary(Name)},
                         {confs, confs_to_binary(Confs)}];
                    Name ->
                        [{name, iolist_to_binary(Name)},
                         {confs, confs_to_binary(Confs)}]
                end
            end, ets:tab2list(emqx_conf_b)),
            lists:map(fun({_, {_Listener, Type, Name}, Status}) ->
                [{type, iolist_to_binary(Type)},
                 {name, iolist_to_binary(Name)},
                 {status, Status}]
            end, ets:tab2list(emqx_listeners_state))}
    end.

confs_to_binary(Confs) ->
    [{iolist_to_binary(Key), iolist_to_binary(Val)} || {Key, Val} <-Confs].

-endif.

import_rule(#{<<"id">> := RuleId,
              <<"rawsql">> := RawSQL,
              <<"actions">> := Actions,
              <<"enabled">> := Enabled,
              <<"description">> := Desc}) ->
    Rule = #{id => RuleId,
             rawsql => RawSQL,
             actions => map_to_actions(Actions),
             enabled => Enabled,
             description => Desc},
    case emqx_rule_engine:create_rule(Rule) of
        {ok, _} -> ok;
        {error, _} ->
            _ = emqx_rule_engine:create_rule(Rule#{enabled => false}),
            ok
    end.

map_to_actions(Maps) ->
    [map_to_action(M) || M <- Maps].

map_to_action(Map = #{<<"id">> := ActionInstId,
                      <<"name">> := <<"data_to_kafka">>,
                      <<"args">> := Args}) ->
    NArgs =
        case maps:get(<<"strategy">>, Args, undefined) of
            <<"first_key_dispatch">> ->
                %% Old version(4.2.x) is first_key_dispatch.
                %% Now is key_dispatch.
                Args#{<<"strategy">> => <<"key_dispatch">>};
            _ ->
                Args
        end,
    #{id => ActionInstId,
      name => 'data_to_kafka',
      args => NArgs,
      fallbacks => map_to_actions(maps:get(<<"fallbacks">>, Map, []))};
map_to_action(Map = #{<<"id">> := ActionInstId, <<"name">> := Name, <<"args">> := Args}) ->
    #{id => ActionInstId,
      name => any_to_atom(Name),
      args => Args,
      fallbacks => map_to_actions(maps:get(<<"fallbacks">>, Map, []))}.


import_rules(Rules) ->
    lists:foreach(fun(Rule) ->
                      import_rule(Rule)
                  end, Rules).

import_resources(Reources) ->
    lists:foreach(fun(Resource) ->
                      import_resource(Resource)
                  end, Reources).

import_resource(#{<<"id">> := Id,
                  <<"type">> := Type,
                  <<"config">> := Config,
                  <<"created_at">> := CreatedAt,
                  <<"description">> := Desc}) ->
    NCreatedAt = case CreatedAt of
                     null -> undefined;
                     _ -> CreatedAt
                 end,
    emqx_rule_engine:create_resource(#{id => Id,
                                       type => any_to_atom(Type),
                                       config => Config,
                                       created_at => NCreatedAt,
                                       description => Desc}).

import_resources_and_rules(Resources, Rules, FromVersion)
  when FromVersion =:= "4.0" orelse
       FromVersion =:= "4.1" orelse
       FromVersion =:= "4.2" orelse
       FromVersion =:= "4.3" ->
    Configs = lists:foldl(fun compatible_version/2 , [], Resources),
    lists:foreach(fun(#{<<"actions">> := Actions} = Rule) ->
                      NActions = apply_new_config(Actions, Configs),
                      import_rule(Rule#{<<"actions">> := NActions})
                  end, Rules);
import_resources_and_rules(Resources, Rules, _FromVersion) ->
    import_resources(Resources),
    import_rules(Rules).

%% 4.2.5 +
compatible_version(#{<<"id">> := ID,
                     <<"type">> := <<"web_hook">>,
                     <<"config">> := #{<<"connect_timeout">> := ConnectTimeout,
                                       <<"content_type">> := ContentType,
                                       <<"headers">> := Headers,
                                       <<"method">> := Method,
                                       <<"pool_size">> := PoolSize,
                                       <<"request_timeout">> := RequestTimeout,
                                       <<"url">> := URL}} = Resource, Acc) ->
    CovertFun = fun(Int) ->
        iolist_to_binary(integer_to_list(Int) ++ "s")
    end,
    Cfg = make_new_config(#{<<"pool_size">> => PoolSize,
                            <<"connect_timeout">> => CovertFun(ConnectTimeout),
                            <<"request_timeout">> => CovertFun(RequestTimeout),
                            <<"url">> => URL}),
    {ok, _Resource} = import_resource(Resource#{<<"config">> := Cfg}),
    NHeaders = maps:put(<<"content-type">>, ContentType, covert_empty_headers(Headers)),
    [{ID, #{headers => NHeaders, method => Method}} | Acc];
% 4.2.0
compatible_version(#{<<"id">> := ID,
                    <<"type">> := <<"web_hook">>,
                    <<"config">> := #{<<"headers">> := Headers,
                                    <<"method">> := Method,%% 4.2.0 Different here
                                    <<"url">> := URL}} = Resource, Acc) ->
    Cfg = make_new_config(#{<<"url">> => URL}),
    {ok, _Resource} = import_resource(Resource#{<<"config">> := Cfg}),
    NHeaders = maps:put(<<"content-type">>, <<"application/json">> , covert_empty_headers(Headers)),
    [{ID, #{headers => NHeaders, method => Method}} | Acc];

%% bridge mqtt
%% 4.2.0 - 4.2.5 bridge_mqtt, ssl enabled from on/off to true/false
compatible_version(#{<<"type">> := <<"bridge_mqtt">>,
                     <<"id">> := ID, %% begin 4.2.0.
                     <<"config">> := #{<<"ssl">> := Ssl} = Config} = Resource, Acc) ->
    NewConfig = Config#{<<"ssl">> := flag_to_boolean(Ssl),
                        <<"pool_size">> => case maps:get(<<"pool_size">>, Config, undefined) of %% 4.0.x, compatible `pool_size`
                                                undefined -> 8;
                                                PoolSize -> PoolSize
                                            end},
    {ok, _Resource} = import_resource(Resource#{<<"config">> := NewConfig}),
    [{ID, NewConfig} | Acc];

% 4.2.3, add :content_type
compatible_version(#{<<"id">> := ID,
                    <<"type">> := <<"web_hook">>,
                    <<"config">> := #{<<"headers">> := Headers,
                                      <<"content_type">> := ContentType,%% 4.2.3 Different here
                                      <<"method">> := Method,
                                      <<"url">> := URL}} = Resource, Acc) ->
    Cfg = make_new_config(#{<<"url">> => URL}),
    {ok, _Resource} = import_resource(Resource#{<<"config">> := Cfg}),
    NHeaders = maps:put(<<"content-type">>, ContentType, covert_empty_headers(Headers)),
    [{ID, #{headers => NHeaders, method => Method}} | Acc];

compatible_version(#{<<"id">> := ID,
                     <<"type">> := Type,
                     <<"config">> := Config} = Resource, Acc)
    when Type =:= <<"backend_mongo_single">>
    orelse Type =:= <<"backend_mongo_sharded">>
    orelse Type =:= <<"backend_mongo_rs">> ->
    NewConfig = maps:merge(#{<<"srv_record">> => false}, Config),
    {ok, _Resource} = import_resource(Resource#{<<"config">> := NewConfig}),
    [{ID, NewConfig} | Acc];

% normal version
compatible_version(Resource, Acc) ->
    {ok, _Resource} = import_resource(Resource),
    Acc.

make_new_config(Cfg) ->
    Config = #{<<"pool_size">> => 8,
               <<"connect_timeout">> => <<"5s">>,
               <<"request_timeout">> => <<"5s">>,
               <<"cacertfile">> => <<>>,
               <<"certfile">> => <<>>,
               <<"keyfile">> => <<>>,
               <<"verify">> => false},
    maps:merge(Cfg, Config).

apply_new_config(Actions, Configs) ->
    apply_new_config(Actions, Configs, []).

apply_new_config([], _Configs, Acc) ->
    Acc;
apply_new_config(Actions, [], []) ->
    Actions;
apply_new_config([Action = #{<<"name">> := <<"data_to_webserver">>,
                             <<"args">> := #{<<"$resource">> := ID,
                                            <<"path">> := Path,
                                            <<"payload_tmpl">> := PayloadTmpl}} | More], Configs, Acc) ->
    case proplists:get_value(ID, Configs, undefined) of
        undefined ->
            apply_new_config(More, Configs, [Action | Acc]);
        #{headers := Headers, method := Method} ->
            Args = #{<<"$resource">> => ID,
                     <<"body">> => PayloadTmpl,
                     <<"headers">> => Headers,
                     <<"method">> => Method,
                     <<"path">> => Path},
            apply_new_config(More, Configs, [Action#{<<"args">> := Args} | Acc])
    end;

apply_new_config([Action = #{<<"args">> := #{<<"$resource">> := ResourceId,
                                             <<"forward_topic">> := ForwardTopic,
                                             <<"payload_tmpl">> := PayloadTmpl},
                           <<"fallbacks">> := _Fallbacks,
                           <<"id">> := _Id,
                           <<"name">> := <<"data_to_mqtt_broker">>} | More], Configs, Acc) ->
            Args = #{<<"$resource">> => ResourceId,
                     <<"payload_tmpl">> => PayloadTmpl,
                     <<"forward_topic">> => ForwardTopic},
            apply_new_config(More, Configs, [Action#{<<"args">> := Args} | Acc]).


actions_to_prop_list(Actions) ->
    [action_to_prop_list(Act) || Act <- Actions].

action_to_prop_list({action_instance, ActionInstId, Name, FallbackActions, Args}) ->
    [{id, ActionInstId},
     {name, Name},
     {fallbacks, actions_to_prop_list(FallbackActions)},
     {args, Args}].

import_blacklist(Blacklist) ->
    lists:foreach(fun(#{<<"who">> := Who,
                        <<"by">> := By,
                        <<"reason">> := Reason,
                        <<"at">> := At,
                        <<"until">> := Until}) ->
                      NWho = case Who of
                                 #{<<"peerhost">> := Peerhost} ->
                                     {ok, NPeerhost} = inet:parse_address(Peerhost),
                                     {peerhost, NPeerhost};
                                 #{<<"clientid">> := ClientId} -> {clientid, ClientId};
                                 #{<<"username">> := Username} -> {username, Username}
                             end,
                     emqx_banned:create(#banned{who = NWho, by = By, reason = Reason, at = At, until = Until})
                  end, Blacklist).

import_applications(Apps) ->
    lists:foreach(fun(#{<<"id">> := AppID,
                        <<"secret">> := AppSecret,
                        <<"name">> := Name,
                        <<"desc">> := Desc,
                        <<"status">> := Status,
                        <<"expired">> := Expired}) ->
                      NExpired = case is_integer(Expired) of
                                     true -> Expired;
                                     false -> undefined
                                 end,
                      emqx_mgmt_auth:force_add_app(AppID, Name, AppSecret, Desc, Status, NExpired)
                  end, Apps).

import_users(Users) ->
    lists:foreach(fun(#{<<"username">> := Username,
                        <<"password">> := Password,
                        <<"tags">> := Tags}) ->
                      NPassword = base64:decode(Password),
                      emqx_dashboard_admin:force_add_user(Username, NPassword, Tags)
                  end, Users).

import_auth_clientid(Lists) ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            lists:foreach(fun(#{<<"clientid">> := Clientid, <<"password">> := Password}) ->
                                  mnesia:dirty_write({emqx_user, {clientid, Clientid}
                                                               , base64:decode(Password)
                                                               , erlang:system_time(millisecond)})
                          end, Lists)
    end.

import_auth_username(Lists) ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            lists:foreach(fun(#{<<"username">> := Username, <<"password">> := Password}) ->
                            mnesia:dirty_write({emqx_user, {username, Username}, base64:decode(Password), erlang:system_time(millisecond)})
                          end, Lists)
    end.

import_auth_mnesia(Auths) ->
    case validate_auth(Auths) of
        ignore -> ok;
        old -> do_import_auth_mnesia_by_old_data(Auths);
        new -> do_import_auth_mnesia(Auths)
    end.

validate_auth(Auths) ->
    case ets:info(emqx_user) of
        undefined -> ignore;
        _ ->
            case lists:all(fun is_new_auth_data/1, Auths) of
                true -> new;
                false ->
                    case lists:all(fun is_old_auth_data/1, Auths) of
                        true ->
                            _ = get_old_type(),
                            old;
                        false -> error({auth_mnesia_data_error, Auths})
                    end
            end
    end.

is_new_auth_data(#{<<"type">> := _, <<"login">> := _, <<"password">> := _}) -> true;
is_new_auth_data(_) -> false.

is_old_auth_data(#{<<"login">> := _, <<"password">> := _} = Auth) ->
    not maps:is_key(<<"type">>, Auth);
is_old_auth_data(_) -> false.

do_import_auth_mnesia_by_old_data(Auths) ->
    CreatedAt = erlang:system_time(millisecond),
    Type = get_old_type(),
    lists:foreach(fun(#{<<"login">> := Login, <<"password">> := Password}) ->
        mnesia:dirty_write({emqx_user, {Type, Login}, base64:decode(Password), CreatedAt})
                  end, Auths).

do_import_auth_mnesia(Auths) ->
    CreatedAt0 = erlang:system_time(millisecond),
    lists:foreach(fun(#{<<"login">> := Login,
        <<"type">> := Type, <<"password">> := Password } = Map) ->
        CreatedAt = maps:get(<<"created_at">>, Map, CreatedAt0),
        mnesia:dirty_write({emqx_user, {any_to_atom(Type), Login}, base64:decode(Password), CreatedAt})
                  end, Auths).

import_acl_mnesia(Acls) ->
    case validate_acl(Acls) of
        ignore -> ok;
        old -> do_import_acl_mnesia_by_old_data(Acls);
        new -> do_import_acl_mnesia(Acls)
    end.

validate_acl(Acls) ->
    case ets:info(emqx_acl2) of
        undefined -> ignore;
        _ ->
            case lists:all(fun is_new_acl_data/1, Acls) of
                true -> new;
                false ->
                    case lists:all(fun is_old_acl_data/1, Acls) of
                        true ->
                            _ = get_old_type(),
                            old;
                        false -> error({acl_mnesia_data_error, Acls})
                    end
            end
    end.

is_new_acl_data(#{<<"action">> := _, <<"access">> := _,
    <<"topic">> := _, <<"type">> := _}) -> true;
is_new_acl_data(_) -> false.

is_old_acl_data(#{<<"login">> := _, <<"topic">> := _,
    <<"allow">> := Allow, <<"action">> := _}) -> is_boolean(any_to_atom(Allow));
is_old_acl_data(_) -> false.

do_import_acl_mnesia_by_old_data(Acls) ->
    lists:foreach(fun(#{<<"login">> := Login,
        <<"topic">> := Topic,
        <<"allow">> := Allow,
        <<"action">> := Action}) ->
        Allow1 = case any_to_atom(Allow) of
                     true -> allow;
                     false -> deny
                 end,
        emqx_acl_mnesia_db:add_acl({get_old_type(), Login}, Topic, any_to_atom(Action), Allow1)
                  end, Acls).

do_import_acl_mnesia(Acls) ->
    lists:foreach(fun(Map = #{<<"action">> := Action,
        <<"access">> := Access, <<"topic">> := Topic}) ->
        Login = case maps:get(<<"type_value">>, Map, undefined) of
                    undefined -> all;
                    Value -> {any_to_atom(maps:get(<<"type">>, Map)), Value}
                end,
        emqx_acl_mnesia_db:add_acl(Login, Topic, any_to_atom(Action), any_to_atom(Access))
                  end, Acls).

-ifdef(EMQX_ENTERPRISE).
import_modules(Modules) ->
    case ets:info(emqx_modules) of
        undefined ->
            ok;
        _ ->
            NModules = migrate_modules(Modules),
            lists:foreach(fun(#{<<"id">> := Id,
                                <<"type">> := Type,
                                <<"config">> := Config,
                                <<"enabled">> := Enabled,
                                <<"created_at">> := CreatedAt,
                                <<"description">> := Description}) ->
                              _ = emqx_modules:import_module({Id, any_to_atom(Type), Config, Enabled, CreatedAt, Description})
                          end, NModules)
    end.

migrate_modules(Modules) ->
    migrate_modules(Modules, []).

migrate_modules([], Acc) ->
    lists:reverse(Acc);
migrate_modules([#{<<"type">> := <<"mongo_authentication">>,
                   <<"config">> := Config} = Module | More], Acc) ->
    WMode = case maps:get(<<"w_mode">>, Config, <<"unsafe">>) of
                <<"undef">> -> <<"unsafe">>;
                Other -> Other
            end,
    RMode = case maps:get(<<"r_mode">>, Config, <<"master">>) of
                <<"undef">> -> <<"master">>;
                <<"slave-ok">> -> <<"slave_ok">>;
                Other0 -> Other0
            end,
    NConfig = Config#{<<"srv_record">> => false,
                      <<"w_mode">> => WMode,
                      <<"r_mode">> => RMode},
    migrate_modules(More, [Module#{<<"config">> => NConfig} | Acc]);
migrate_modules([Module | More], Acc) ->
    migrate_modules(More, [Module | Acc]).

import_schemas(Schemas) ->
    case ets:info(emqx_schema) of
        undefined -> ok;
        _ -> [emqx_schema_registry:add_schema(emqx_schema_api:make_schema_params(Schema)) || Schema <- Schemas]
    end.

import_confs(Configs, ListenersState) ->
    case ets:info(emqx_conf_info) of
        undefined -> ok;
        _ ->
            emqx_conf:import_confs(Configs, ListenersState)
    end.

-endif.

any_to_atom(L) when is_list(L) -> list_to_atom(L);
any_to_atom(B) when is_binary(B) -> binary_to_atom(B, utf8);
any_to_atom(A) when is_atom(A) -> A.

to_version(Version) when is_integer(Version) ->
    integer_to_list(Version);
to_version(Version) when is_binary(Version) ->
    binary_to_list(Version);
to_version(Version) when is_list(Version) ->
    Version.

%% TODO: do not allow abs file path here.
%% i.e. Filename0 should be a relative path only
%% or the path prefix is in an white-list
upload_backup_file(Filename0, Bin) ->
    %% ensure it's a binary, so filenmae:join will always return binary
    Filename1 = to_unicode_bin(Filename0),
    case ensure_file_name(Filename1) of
        {ok, Filename} ->
            case check_json(Bin) of
                {ok, _} ->
                    ok = filelib:ensure_dir(Filename),
                    logger:info("write backup file ~p", [Filename]),
                    file:write_file(Filename, Bin);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

list_backup_file() ->
    Filter =
        fun(File) ->
            case file:read_file_info(File) of
                {ok, #file_info{size = Size, ctime = CTime = {{Y, M, D}, {H, MM, S}}}} ->
                    Seconds = calendar:datetime_to_gregorian_seconds(CTime),
                    BaseFilename = to_unicode_bin(filename:basename(File)),
                    CreatedAt = to_unicode_bin(io_lib:format("~p-~p-~p ~p:~p:~p", [Y, M, D, H, MM, S])),
                    Info = {
                        Seconds,
                        [{filename, BaseFilename},
                         {size, Size},
                         {created_at, CreatedAt},
                         {node, node()}
                        ]
                    },
                    {true, Info};
                _ ->
                    false
            end
        end,
    lists:filtermap(Filter, backup_files()).

backup_files() ->
    backup_files(backup_dir()) ++
        backup_files(backup_dir_old_version()).

backup_files(Dir) ->
    {ok, FilesAll} = file:list_dir_all(Dir),
    Files = lists:filtermap(fun legal_filename/1, FilesAll),
    [filename:join([Dir, to_unicode_bin(File)]) || File <- Files].

look_up_file(Filename) ->
    DefOnNotFound = fun(_Filename) -> {error, not_found} end,
    do_look_up_file(Filename, DefOnNotFound).

do_look_up_file(Filename, OnNotFound) ->
    Filter =
        fun(MaybeFile) ->
            filename:basename(MaybeFile) =:= Filename
        end,
    case lists:filter(Filter, backup_files()) of
        [] ->
            OnNotFound(Filename);
        List ->
            {ok, hd(List)}
    end.

read_backup_file(Filename0) ->
    case look_up_file(Filename0) of
        {ok, Filename} ->
            case file:read_file(Filename) of
                {ok, Bin} ->
                    {ok, #{filename => to_unicode_bin(Filename0),
                           file => Bin}};
                {error, Reason} ->
                    logger:error("read file ~p failed ~p", [Filename, Reason]),
                    {error, bad_file}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

delete_backup_file(Filename0) ->
    case look_up_file(Filename0) of
        {ok, Filename} ->
            case file:read_file_info(Filename) of
                {ok, #file_info{}} ->
                    case file:delete(Filename) of
                        ok ->
                            logger:info("delete backup file ~p", [Filename]),
                            ok;
                        {error, Reason} ->
                            logger:error(
                                "delete backup file ~p error:~p", [Filename, Reason]),
                            {error, Reason}
                    end;
                _ ->
                    {error, not_found}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

-ifdef(TEST).
%% clean all for test
delete_all_backup_file() ->
    [begin
        Filename = proplists:get_value(filename, Info),
        _ = delete_backup_file(Filename)
    end || {_, Info} <- list_backup_file()],
    ok.
-endif.

export() ->
    Seconds = erlang:system_time(second),
    Data = do_export_data() ++ [{date, iolist_to_binary(emqx_mgmt_util:strftime(Seconds))}],
    {{Y, M, D}, {H, MM, S}} = emqx_mgmt_util:datetime(Seconds),
    BaseFilename = to_unicode_bin(io_lib:format("emqx-export-~p-~p-~p-~p-~p-~p.json", [Y, M, D, H, MM, S])),
    {ok, Filename} = ensure_file_name(BaseFilename),
    case file:write_file(Filename, emqx_json:encode(Data)) of
        ok ->
            case file:read_file_info(Filename) of
                {ok, #file_info{size = Size, ctime = {{Y1, M1, D1}, {H1, MM1, S1}}}} ->
                    CreatedAt = io_lib:format("~p-~p-~p ~p:~p:~p", [Y1, M1, D1, H1, MM1, S1]),
                    {ok, #{filename => Filename,
                           size => Size,
                           created_at => iolist_to_binary(CreatedAt),
                           node => node()
                          }};
                Error -> Error
            end;
        Error -> Error
    end.

do_export_data() ->
    Version = string:sub_string(emqx_sys:version(), 1, 3),
    [{version, iolist_to_binary(Version)},
     {rules, export_rules()},
     {resources, export_resources()},
     {blacklist, export_blacklist()},
     {apps, export_applications()},
     {users, export_users()},
     {auth_mnesia, export_auth_mnesia()},
     {acl_mnesia, export_acl_mnesia()}
     ] ++ do_export_extra_data().

-ifdef(EMQX_ENTERPRISE).
do_export_extra_data() ->
    {Configs, State} = export_confs(),
    [{modules, export_modules()},
     {schemas, export_schemas()},
     {configs, Configs},
     {listeners_state, State}
    ].
-else.
do_export_extra_data() -> [].
-endif.

-ifdef(EMQX_ENTERPRISE).
import(Filename, OverridesJson) ->
    case check_import_json(Filename) of
        {ok, Imported} ->
            Overrides = emqx_json:decode(OverridesJson, [return_maps]),
            Data = maps:merge(Imported, Overrides),
            Version = to_version(maps:get(<<"version">>, Data)),
            read_global_auth_type(Data, Version),
            try
                do_import_data(Data, Version),
                logger:debug("The emqx data has been imported successfully"),
                ok
            catch Class:Reason:Stack ->
                logger:error("The emqx data import failed: ~0p", [{Class, Reason, Stack}]),
                {error, import_failed}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
-else.
import(Filename, OverridesJson) ->
    case check_import_json(Filename) of
        {ok, Imported} ->
            Overrides = emqx_json:decode(OverridesJson, [return_maps]),
            Data = maps:merge(Imported, Overrides),
            Version = to_version(maps:get(<<"version">>, Data)),
            read_global_auth_type(Data, Version),
            case is_version_supported(Data, Version) of
                true  ->
                    try
                        do_import_data(Data, Version),
                        logger:debug("The emqx data has been imported successfully"),
                        ok
                    catch Class:Reason:Stack ->
                        logger:error("The emqx data import failed: ~0p", [{Class, Reason, Stack}]),
                        {error, import_failed}
                    end;
                false ->
                    logger:error("Unsupported version: ~p", [Version]),
                    {error, unsupported_version, Version}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
-endif.

-spec(check_import_json(binary() | string()) -> {ok, map()} | {error, term()}).
check_import_json(Filename) when is_list(Filename) ->
    check_import_json(to_unicode_bin(Filename));
check_import_json(Filename) ->
    OnNotFound =
        fun(F) ->
                case filelib:is_file(F) of
                    true -> {ok, F};
                    false -> {error, not_found}
                end
        end,
    FunList = [
        fun(F) -> do_look_up_file(F, OnNotFound) end,
        fun(F) -> file:read_file(F) end,
        fun check_json/1
    ],
    do_check_import_json(Filename, FunList).

do_check_import_json(Res, []) ->
    {ok, Res};
do_check_import_json(Acc, [Fun | FunList]) ->
    case Fun(Acc) of
        {ok, Next} ->
            do_check_import_json(Next, FunList);
        {error, Reason} ->
            {error, Reason}
    end.

ensure_file_name(Filename) ->
    case legal_filename(Filename) of
        true ->
            {ok, filename:join(backup_dir(), Filename)};
        false ->
            {error, bad_filename}
    end.

backup_dir() ->
    Dir = filename:join(emqx:get_env(data_dir), ?BACKUP_DIR),
    ok = filelib:ensure_dir(filename:join([Dir, dummy])),
    Dir.

backup_dir_old_version() ->
    emqx:get_env(data_dir).

legal_filename(Filename) ->
    MaybeJson = filename:extension(Filename),
    MaybeJson == ".json" orelse MaybeJson == <<".json">>.

check_json(MaybeJson) ->
    case emqx_json:safe_decode(MaybeJson, [return_maps]) of
        {ok, Json} ->
            {ok, Json};
        {error, _} ->
            {error, bad_json}
    end.

do_import_data(Data, Version) ->
    import_resources_and_rules(maps:get(<<"resources">>, Data, []), maps:get(<<"rules">>, Data, []), Version),
    import_blacklist(maps:get(<<"blacklist">>, Data, [])),
    import_applications(maps:get(<<"apps">>, Data, [])),
    import_users(maps:get(<<"users">>, Data, [])),
    %% Import modules first to ensure the data of auth_mnesia module can be imported.
    %% XXX: In opensource version, can't import if the emqx_auth_mnesia plug-in is not started??
    do_import_enterprise_modules(Data, Version),
    import_auth_clientid(maps:get(<<"auth_clientid">>, Data, [])),
    import_auth_username(maps:get(<<"auth_username">>, Data, [])),
    import_auth_mnesia(maps:get(<<"auth_mnesia">>, Data, [])),
    import_acl_mnesia(maps:get(<<"acl_mnesia">>, Data, [])),
    %% always do extra import at last, to make sure resources are initiated before
    %% creating the schemas
    do_import_extra_data(Data, Version).

-ifdef(EMQX_ENTERPRISE).
do_import_extra_data(Data, _Version) ->
    _ = import_confs(maps:get(<<"configs">>, Data, []), maps:get(<<"listeners_state">>, Data, [])),
    _ = import_schemas(maps:get(<<"schemas">>, Data, [])),
    ok.
-else.
do_import_extra_data(_Data, _Version) -> ok.
-endif.

-ifdef(EMQX_ENTERPRISE).
do_import_enterprise_modules(Data, _Version) ->
    _ = import_modules(maps:get(<<"modules">>, Data, [])),
    ok.
-else.
do_import_enterprise_modules(_Data, _Version) -> ok.
-endif.

covert_empty_headers([]) -> #{};
covert_empty_headers(Other) -> Other.

flag_to_boolean(<<"on">>) -> true;
flag_to_boolean(<<"off">>) -> false;
flag_to_boolean(Other) -> Other.

-ifndef(EMQX_ENTERPRISE).
is_version_supported(Data, Version) ->
    case { maps:get(<<"auth_clientid">>, Data, [])
         , maps:get(<<"auth_username">>, Data, [])
         , maps:get(<<"auth_mnesia">>, Data, [])} of
        {[], [], []} -> lists:member(Version, ?VERSIONS);
        _ -> is_version_supported2(Version)
    end.

is_version_supported2("4.1") ->
    true;
is_version_supported2("4.3") ->
    true;
is_version_supported2("4.4") ->
    true;
is_version_supported2("4.5") ->
    true;
is_version_supported2(Version) ->
    case re:run(Version, "^4.[02].\\d+$", [{capture, none}]) of
        match ->
            try lists:map(fun erlang:list_to_integer/1, string:tokens(Version, ".")) of
                [4, 2, N] -> N >= 11;
                [4, 0, N] -> N >= 13;
                _ -> false
            catch
                _ : _ -> false
            end;
        nomatch ->
            false
    end.
-endif.

read_global_auth_type(Data, Version) ->
    case {maps:get(<<"auth_mnesia">>, Data, []), maps:get(<<"acl_mnesia">>, Data, [])} of
        {[], []} ->
            %% Auth mnesia plugin is not used:
            ok;
        _ ->
            do_read_global_auth_type(Data, Version)
    end.

-ifdef(EMQX_ENTERPRISE).
do_read_global_auth_type(Data, _Version) ->
    case Data of
        #{<<"auth.mnesia.as">> := <<"username">>} ->
            set_old_type(username);
        #{<<"auth.mnesia.as">> := <<"clientid">>} ->
            set_old_type(clientid);
        _ ->
            ok
    end.

-else.
do_read_global_auth_type(Data, FromVersion) ->
    case Data of
        #{<<"auth.mnesia.as">> := <<"username">>} ->
            set_old_type(username);
        #{<<"auth.mnesia.as">> := <<"clientid">>} ->
            set_old_type(clientid);
        _ when FromVersion =:= "4.0" orelse
            FromVersion =:= "4.1" orelse
            FromVersion =:= "4.2"->
            logger:error("While importing data from EMQX versions prior to 4.3 "
                         "it is necessary to specify the value of \"auth.mnesia.as\" parameter "
                         "as it was configured in etc/plugins/emqx_auth_mnesia.conf.\n"
                         "Use the following command to import data:\n"
                         "  $ emqx_ctl data import <filename> --env '{\"auth.mnesia.as\":\"username\"}'\n"
                         "or\n"
                         "  $ emqx_ctl data import <filename> --env '{\"auth.mnesia.as\":\"clientid\"}'",
                         []),
            error({import_failed, FromVersion});
        _ ->
            ok
    end.
-endif.

get_old_type() ->
    {ok, Type} = application:get_env(emqx_auth_mnesia, as),
    Type.

set_old_type(Type) ->
    application:set_env(emqx_auth_mnesia, as, Type).

to_unicode_bin(Bin) when is_binary(Bin) -> Bin;
to_unicode_bin(Str) when is_list(Str) -> unicode:characters_to_binary(Str).
