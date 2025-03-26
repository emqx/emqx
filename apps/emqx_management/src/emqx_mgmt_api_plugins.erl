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
-module(emqx_mgmt_api_plugins).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_plugins/include/emqx_plugins.hrl").
-include_lib("erlavro/include/erlavro.hrl").

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    list_plugins/2,
    upload_install/2,
    plugin/2,
    update_plugin/2,
    plugin_config/2,
    plugin_schema/2,
    update_boot_order/2
]).

-export([
    validate_name/1,
    validate_file_name/2,
    get_plugins/0,
    install_package/2,
    delete_package/1,
    delete_package/2,
    describe_package/1,
    ensure_action/2,
    ensure_action/3,
    do_update_plugin_config/3
]).

-define(NAME_RE, "^[A-Za-z]+\\w*\\-[\\w-.]*$").
-define(TAGS, [<<"Plugins">>]).

-define(CONTENT_PLUGIN, plugin).

namespace() ->
    "plugins".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

%% Don't change the path's order
paths() ->
    [
        "/plugins",
        "/plugins/:name",
        "/plugins/install",
        "/plugins/:name/:action",
        "/plugins/:name/config",
        "/plugins/:name/schema",
        "/plugins/:name/move"
    ].

schema("/plugins") ->
    #{
        'operationId' => list_plugins,
        get => #{
            summary => <<"List all installed plugins">>,
            description =>
                "Plugins are launched in top-down order.<br/>"
                "Use `POST /plugins/{name}/move` to change the boot order.",
            tags => ?TAGS,
            responses => #{
                200 => hoconsc:array(hoconsc:ref(plugin))
            }
        }
    };
schema("/plugins/install") ->
    #{
        'operationId' => upload_install,
        filter => fun ?MODULE:validate_file_name/2,
        post => #{
            summary => <<"Install a new plugin">>,
            description =>
                "Upload a plugin tarball (plugin-vsn.tar.gz)."
                "Follow [emqx-plugin-template](https://github.com/emqx/emqx-plugin-template) "
                "to develop plugin.",
            tags => ?TAGS,
            'requestBody' => #{
                content => #{
                    'multipart/form-data' => #{
                        schema => #{
                            type => object,
                            properties => #{
                                ?CONTENT_PLUGIN => #{type => string, format => binary}
                            }
                        },
                        encoding => #{?CONTENT_PLUGIN => #{'contentType' => 'application/gzip'}}
                    }
                }
            },
            responses => #{
                204 => <<"Install plugin successfully">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [
                        'UNEXPECTED_ERROR',
                        'ALREADY_INSTALLED',
                        'BAD_PLUGIN_INFO',
                        'BAD_FORM_DATA',
                        'FORBIDDEN'
                    ]
                )
            }
        }
    };
schema("/plugins/:name") ->
    #{
        'operationId' => plugin,
        get => #{
            summary => <<"Get a plugin description">>,
            description => "Describe a plugin according to its `release.json` and `README.md`.",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:ref(plugin),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        },
        delete => #{
            summary => <<"Delete a plugin">>,
            description => "Uninstalls a previously uploaded plugin package.",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => <<"Uninstall successfully">>,
                400 => emqx_dashboard_swagger:error_codes(['PARAM_ERROR'], <<"Bad parameter">>),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        }
    };
schema("/plugins/:name/:action") ->
    #{
        'operationId' => update_plugin,
        put => #{
            summary => <<"Trigger action on an installed plugin">>,
            description =>
                "start/stop a installed plugin.<br/>"
                "- **start**: start the plugin.<br/>"
                "- **stop**: stop the plugin.<br/>",
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(name),
                {action, hoconsc:mk(hoconsc:enum([start, stop]), #{desc => "Action", in => path})}
            ],
            responses => #{
                204 => <<"Trigger action successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        }
    };
schema("/plugins/:name/config") ->
    #{
        'operationId' => plugin_config,
        get => #{
            summary => <<"Get plugin config">>,
            description =>
                "Get plugin config. Config schema is defined by user's schema.avsc file.<br/>",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                %% avro data, json encoded
                200 => hoconsc:mk(binary()),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG'], <<"Plugin Config Not Found">>
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        },
        put => #{
            summary =>
                <<"Update plugin config">>,
            description =>
                "Update plugin config. Config schema defined by user's schema.avsc file.<br/>",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object
                        }
                    }
                }
            },
            responses => #{
                204 => <<"Config updated successfully">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG', 'UNEXPECTED_ERROR'], <<"Update plugin config failed">>
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        }
    };
schema("/plugins/:name/schema") ->
    #{
        'operationId' => plugin_schema,
        get => #{
            summary => <<"Get installed plugin's AVRO schema">>,
            description => "Get plugin's config AVRO schema.",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                %% avro schema and i18n json object
                200 => hoconsc:mk(binary()),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND', 'FILE_NOT_EXISTED'],
                    <<"Plugin Not Found or Plugin not given a schema file">>
                )
            }
        }
    };
schema("/plugins/:name/move") ->
    #{
        'operationId' => update_boot_order,
        post => #{
            summary => <<"Move plugin within plugin hierarchy">>,
            description => "Setting the boot order of plugins.",
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => move_request_body(),
            responses => #{
                204 => <<"Boot order changed successfully">>,
                400 => emqx_dashboard_swagger:error_codes(['MOVE_FAILED'], <<"Move failed">>)
            }
        }
    }.

fields(plugin) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => "Name-Vsn: without .tar.gz",
                    validator => fun ?MODULE:validate_name/1,
                    required => true,
                    example => "emqx_plugin_template-5.0-rc.1"
                }
            )},
        {author, hoconsc:mk(list(string()), #{example => [<<"EMQX Team">>]})},
        {builder, hoconsc:ref(?MODULE, builder)},
        {built_on_otp_release, hoconsc:mk(string(), #{example => "24"})},
        {compatibility, hoconsc:mk(map(), #{example => #{<<"emqx">> => <<"~>5.0">>}})},
        {git_commit_or_build_date,
            hoconsc:mk(string(), #{
                example => "2021-12-25",
                desc =>
                    "Last git commit date by `git log -1 --pretty=format:'%cd' "
                    "--date=format:'%Y-%m-%d`.\n"
                    " If the last commit date is not available, the build date will be presented."
            })},
        {functionality, hoconsc:mk(hoconsc:array(string()), #{example => [<<"Demo">>]})},
        {git_ref, hoconsc:mk(string(), #{example => "ddab50fafeed6b1faea70fc9ffd8c700d7e26ec1"})},
        {metadata_vsn, hoconsc:mk(string(), #{example => "0.1.0"})},
        {rel_vsn,
            hoconsc:mk(
                binary(),
                #{
                    desc => "Plugins release version",
                    required => true,
                    example => <<"5.0-rc.1">>
                }
            )},
        {rel_apps,
            hoconsc:mk(
                hoconsc:array(binary()),
                #{
                    desc => "Aplications in plugin.",
                    required => true,
                    example => [<<"emqx_plugin_template-5.0.0">>, <<"map_sets-1.1.0">>]
                }
            )},
        {repo, hoconsc:mk(string(), #{example => "https://github.com/emqx/emqx-plugin-template"})},
        {description,
            hoconsc:mk(
                binary(),
                #{
                    desc => "Plugin description.",
                    required => true,
                    example => "This is an demo plugin description"
                }
            )},
        {running_status,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(running_status)),
                #{required => true}
            )},
        {readme,
            hoconsc:mk(binary(), #{
                example => "This is an demo plugin.",
                desc => "only return when `GET /plugins/{name}`.",
                required => false
            })}
    ];
fields(name) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => list_to_binary(?NAME_RE),
                    example => "emqx_plugin_template-5.0-rc.1",
                    in => path,
                    validator => fun ?MODULE:validate_name/1
                }
            )}
    ];
fields(builder) ->
    [
        {contact, hoconsc:mk(string(), #{example => "emqx-support@emqx.io"})},
        {name, hoconsc:mk(string(), #{example => "EMQX Team"})},
        {website, hoconsc:mk(string(), #{example => "www.emqx.com"})}
    ];
fields(position) ->
    [
        {position,
            hoconsc:mk(
                hoconsc:union([front, rear, binary()]),
                #{
                    desc =>
                        ""
                        "\n"
                        "             Enable auto-boot at position in the boot list, where Position could be\n"
                        "             'front', 'rear', or 'before:other-vsn', 'after:other-vsn'\n"
                        "             to specify a relative position.\n"
                        "            "
                        "",
                    required => false
                }
            )}
    ];
fields(running_status) ->
    [
        {node, hoconsc:mk(string(), #{example => "emqx@127.0.0.1"})},
        {status,
            hoconsc:mk(hoconsc:enum([running, stopped]), #{
                desc =>
                    "Install plugin status at runtime<br/>"
                    "1. running: plugin is running.<br/>"
                    "2. stopped: plugin is stopped.<br/>"
            })}
    ].

move_request_body() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:ref(?MODULE, position),
        #{
            move_to_front => #{
                summary => <<"move plugin on the front">>,
                value => #{position => <<"front">>}
            },
            move_to_rear => #{
                summary => <<"move plugin on the rear">>,
                value => #{position => <<"rear">>}
            },
            move_to_before => #{
                summary => <<"move plugin before other plugins">>,
                value => #{position => <<"before:emqx_plugin_demo-5.1-rc.2">>}
            },
            move_to_after => #{
                summary => <<"move plugin after other plugins">>,
                value => #{position => <<"after:emqx_plugin_demo-5.1-rc.2">>}
            }
        }
    ).

validate_name(Name) ->
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, ?NAME_RE) of
                nomatch ->
                    {
                        error,
                        "Name should be an application name"
                        " (starting with a letter, containing letters, digits and underscores)"
                        " followed with a dash and a version string "
                        " (can contain letters, digits, dots, and dashes), "
                        " e.g. emqx_plugin_template-5.0-rc.1"
                    };
                _ ->
                    ok
            end;
        false ->
            {error, "Name Length must =< 256"}
    end.

validate_file_name(#{body := #{<<"plugin">> := Plugin}} = Params, _Meta) when is_map(Plugin) ->
    [{FileName, Bin}] = maps:to_list(maps:without([type], Plugin)),
    NameVsn = string:trim(FileName, trailing, ".tar.gz"),
    case validate_name(NameVsn) of
        ok ->
            {ok, Params#{name => NameVsn, bin => Bin}};
        {error, Reason} ->
            {400, #{
                code => 'BAD_PLUGIN_INFO',
                message => iolist_to_binary(["Bad plugin file name: ", FileName, ". ", Reason])
            }}
    end;
validate_file_name(_Params, _Meta) ->
    {400, #{
        code => 'BAD_FORM_DATA',
        message =>
            <<"form-data should be `plugin=@packagename-vsn.tar.gz;type=application/x-gzip`">>
    }}.

%% API CallBack Begin
list_plugins(get, _) ->
    Nodes = emqx:running_nodes(),
    {Plugins, []} = emqx_mgmt_api_plugins_proto_v3:get_plugins(Nodes),
    {200, format_plugins(Plugins)}.

get_plugins() ->
    {node(), emqx_plugins:list()}.

upload_install(post, #{name := NameVsn, bin := Bin}) ->
    case emqx_plugins:describe(NameVsn) of
        {error, #{msg := "bad_info_file", reason := {enoent, _Path}}} ->
            case emqx_plugins_fs:is_tar_present(NameVsn) of
                false ->
                    install_package_on_nodes(NameVsn, Bin);
                {true, TarGzs} ->
                    %% TODO
                    %% What if a tar file is present but is not unpacked, i.e.
                    %% the plugin is not fully installed?
                    {400, #{
                        code => 'ALREADY_INSTALLED',
                        message => iolist_to_binary(io_lib:format("~p already installed", [TarGzs]))
                    }}
            end;
        {ok, _} ->
            {400, #{
                code => 'ALREADY_INSTALLED',
                message => iolist_to_binary(io_lib:format("~p is already installed", [NameVsn]))
            }}
    end;
upload_install(post, #{}) ->
    {400, #{
        code => 'BAD_FORM_DATA',
        message =>
            <<"form-data should be `plugin=@packagename-vsn.tar.gz;type=application/x-gzip`">>
    }}.

install_package_on_nodes(NameVsn, Bin) ->
    case emqx_plugins:is_allowed_installation(NameVsn) of
        true ->
            do_install_package_on_nodes(NameVsn, Bin);
        false ->
            Msg = iolist_to_binary([
                <<"Package is not allowed installation;">>,
                <<" first allow it to be installed by running:">>,
                <<" `emqx ctl plugins allow ">>,
                NameVsn,
                <<"`">>
            ]),
            {403, #{code => 'FORBIDDEN', message => Msg}}
    end.

do_install_package_on_nodes(NameVsn, Bin) ->
    %% TODO: handle bad nodes
    Nodes = emqx:running_nodes(),
    {[_ | _] = Res, []} = emqx_mgmt_api_plugins_proto_v3:install_package(Nodes, NameVsn, Bin),
    case lists:filter(fun(R) -> R =/= ok end, Res) of
        [] ->
            {204};
        Filtered ->
            %% crash if we have unexpected errors or results
            [] = lists:filter(
                fun
                    ({error, {failed, _}}) -> true;
                    ({error, _}) -> false
                end,
                Filtered
            ),
            Reason =
                case hd(Filtered) of
                    {error, #{msg := Reason0}} -> Reason0;
                    {error, #{reason := Reason0}} -> Reason0
                end,
            {400, #{
                code => 'BAD_PLUGIN_INFO',
                message => iolist_to_binary([bin(Reason), ": ", NameVsn])
            }}
    end.

plugin(get, #{bindings := #{name := NameVsn}}) ->
    Nodes = emqx:running_nodes(),
    {Plugins, _} = emqx_mgmt_api_plugins_proto_v3:describe_package(Nodes, NameVsn),
    case format_plugins(Plugins) of
        [Plugin] -> {200, Plugin};
        [] -> {404, #{code => 'NOT_FOUND', message => NameVsn}}
    end;
plugin(delete, #{bindings := #{name := NameVsn}}) ->
    Res = emqx_mgmt_api_plugins_proto_v3:delete_package(NameVsn),
    return(204, Res).

update_plugin(put, #{bindings := #{name := NameVsn, action := Action}}) ->
    Res = emqx_mgmt_api_plugins_proto_v3:ensure_action(NameVsn, Action),
    return(204, Res).

plugin_config(get, #{bindings := #{name := NameVsn}}) ->
    case emqx_plugins:describe(NameVsn) of
        {ok, _} ->
            case emqx_plugins:get_config(NameVsn, ?CONFIG_FORMAT_MAP, ?plugin_conf_not_found) of
                {ok, Config} when is_map(Config) ->
                    {200, #{<<"content-type">> => <<"'application/json'">>}, Config};
                {ok, ?plugin_conf_not_found} ->
                    {400, #{
                        code => 'BAD_CONFIG',
                        message => <<"Plugin Config Not Found">>
                    }}
            end;
        _ ->
            {404, plugin_not_found_msg()}
    end;
plugin_config(put, #{bindings := #{name := NameVsn}, body := AvroJsonMap}) ->
    Nodes = emqx:running_nodes(),
    case emqx_plugins:describe(NameVsn) of
        {ok, _} ->
            case emqx_plugins:decode_plugin_config_map(NameVsn, AvroJsonMap) of
                {ok, ?plugin_without_config_schema} ->
                    %% no plugin avro schema, just put the json map as-is
                    _Res = emqx_mgmt_api_plugins_proto_v3:update_plugin_config(
                        Nodes, NameVsn, AvroJsonMap, ?plugin_without_config_schema
                    ),
                    {204};
                {ok, AvroValue} ->
                    %% cluster call with config in map (binary key-value)
                    _Res = emqx_mgmt_api_plugins_proto_v3:update_plugin_config(
                        Nodes, NameVsn, AvroJsonMap, AvroValue
                    ),
                    {204};
                {error, Reason} ->
                    {400, #{
                        code => 'BAD_CONFIG',
                        message => readable_error_msg(Reason)
                    }}
            end;
        _ ->
            {404, plugin_not_found_msg()}
    end.

plugin_schema(get, #{bindings := #{name := NameVsn}}) ->
    case emqx_plugins:describe(NameVsn) of
        {ok, _Plugin} ->
            {200, format_plugin_avsc_and_i18n(NameVsn)};
        _ ->
            {404, plugin_not_found_msg()}
    end.

update_boot_order(post, #{bindings := #{name := Name}, body := Body}) ->
    case parse_position(Body, Name) of
        {error, Reason} ->
            {400, #{code => 'BAD_POSITION', message => Reason}};
        Position ->
            case emqx_plugins:ensure_enabled(Name, Position, global) of
                ok ->
                    {204};
                {error, Reason} ->
                    {400, #{
                        code => 'MOVE_FAILED',
                        message => readable_error_msg(Reason)
                    }}
            end
    end.

%% API CallBack End

%% For RPC upload_install/2
install_package(NameVsn, Bin) ->
    ok = emqx_plugins_fs:write_tar(NameVsn, Bin),
    case emqx_plugins:ensure_installed(NameVsn, ?fresh_install) of
        {error, #{reason := plugin_not_found}} = NotFound ->
            NotFound;
        {error, Reason} = Error ->
            ?SLOG(error, Reason#{msg => "failed_to_install_plugin"}),
            _ = emqx_plugins_fs:delete_tar(NameVsn),
            Error;
        Result ->
            Result
    end.

%% For RPC plugin get
describe_package(NameVsn) ->
    Node = node(),
    case emqx_plugins:describe(NameVsn) of
        {ok, Plugin} -> {Node, [Plugin]};
        _ -> {Node, []}
    end.

%% Tip: Don't delete delete_package/1, use before v571 cluster_rpc
delete_package(NameVsn) ->
    delete_package(NameVsn, #{}).

%% For RPC plugin delete
delete_package(NameVsn, _Opts) ->
    _ = emqx_plugins:forget_allowed_installation(NameVsn),
    case emqx_plugins:ensure_stopped(NameVsn) of
        ok ->
            _ = emqx_plugins:ensure_disabled(NameVsn),
            _ = emqx_plugins:ensure_uninstalled(NameVsn),
            _ = emqx_plugins:delete_package(NameVsn),
            ok;
        Error ->
            Error
    end.

%% Tip: Don't delete ensure_action/2, use before v571 cluster_rpc
ensure_action(Name, Action) ->
    ensure_action(Name, Action, #{}).

%% for RPC plugin update
%% TODO: catch thrown error to return 400
%% - plugin_not_found
%% - otp vsn assertion failed

ensure_action(Name, start, _Opts) ->
    _ = emqx_plugins:ensure_started(Name),
    _ = emqx_plugins:ensure_enabled(Name),
    ok;
ensure_action(Name, stop, _Opts) ->
    _ = emqx_plugins:ensure_stopped(Name),
    _ = emqx_plugins:ensure_disabled(Name),
    ok;
ensure_action(Name, restart, _Opts) ->
    _ = emqx_plugins:ensure_enabled(Name),
    _ = emqx_plugins:restart(Name),
    ok.

%% for RPC plugin avro encoded config update
-spec do_update_plugin_config(name_vsn(), map(), any()) ->
    ok.
do_update_plugin_config(NameVsn, AvroJsonMap, _AvroValue) ->
    emqx_plugins:put_config(NameVsn, AvroJsonMap).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

return(Code, ok) ->
    {Code};
return(_, {error, #{msg := Msg, reason := {enoent, Path} = Reason}}) ->
    ?SLOG(error, #{msg => Msg, reason => Reason}),
    {404, #{code => 'NOT_FOUND', message => iolist_to_binary([Path, " does not exist"])}};
return(_, {error, Reason}) ->
    {400, #{code => 'PARAM_ERROR', message => readable_error_msg(Reason)}}.

plugin_not_found_msg() ->
    #{
        code => 'NOT_FOUND',
        message => <<"Plugin Not Found">>
    }.

readable_error_msg(Msg) ->
    emqx_utils:readable_error_msg(Msg).

parse_position(#{<<"position">> := <<"front">>}, _) ->
    front;
parse_position(#{<<"position">> := <<"rear">>}, _) ->
    rear;
parse_position(#{<<"position">> := <<"before:", Name/binary>>}, Name) ->
    {error, <<"Invalid parameter. Cannot be placed before itself">>};
parse_position(#{<<"position">> := <<"after:", Name/binary>>}, Name) ->
    {error, <<"Invalid parameter. Cannot be placed after itself">>};
parse_position(#{<<"position">> := <<"before:">>}, _Name) ->
    {error, <<"Invalid parameter. Cannot be placed before an empty target">>};
parse_position(#{<<"position">> := <<"after:">>}, _Name) ->
    {error, <<"Invalid parameter. Cannot be placed after an empty target">>};
parse_position(#{<<"position">> := <<"before:", Before/binary>>}, _Name) ->
    {before, binary_to_list(Before)};
parse_position(#{<<"position">> := <<"after:", After/binary>>}, _Name) ->
    {behind, binary_to_list(After)};
parse_position(Position, _) ->
    {error, iolist_to_binary(io_lib:format("~p", [Position]))}.

format_plugins(List) ->
    StatusMap = aggregate_status(List),
    SortFun = fun({_N1, P1}, {_N2, P2}) -> length(P1) > length(P2) end,
    SortList = lists:sort(SortFun, List),
    pack_status_in_order(SortList, StatusMap).

pack_status_in_order(List, StatusMap) ->
    {Plugins, _} =
        lists:foldl(
            fun({_Node, PluginList}, {Acc, StatusAcc}) ->
                pack_plugin_in_order(PluginList, Acc, StatusAcc)
            end,
            {[], StatusMap},
            List
        ),
    lists:reverse(Plugins).

pack_plugin_in_order([], Acc, StatusAcc) ->
    {Acc, StatusAcc};
pack_plugin_in_order(_, Acc, StatusAcc) when map_size(StatusAcc) =:= 0 -> {Acc, StatusAcc};
pack_plugin_in_order([Plugin0 | Plugins], Acc, StatusAcc) ->
    #{<<"name">> := Name, <<"rel_vsn">> := Vsn} = Plugin0,
    case maps:find({Name, Vsn}, StatusAcc) of
        {ok, Status} ->
            Plugin1 = maps:without([running_status, config_status], Plugin0),
            Plugins2 = Plugin1#{running_status => Status},
            NewStatusAcc = maps:remove({Name, Vsn}, StatusAcc),
            pack_plugin_in_order(Plugins, [Plugins2 | Acc], NewStatusAcc);
        error ->
            pack_plugin_in_order(Plugins, Acc, StatusAcc)
    end.

aggregate_status(List) -> aggregate_status(List, #{}).

aggregate_status([], Acc) ->
    Acc;
aggregate_status([{Node, Plugins} | List], Acc) ->
    NewAcc =
        lists:foldl(
            fun(Plugin, SubAcc) ->
                #{<<"name">> := Name, <<"rel_vsn">> := Vsn} = Plugin,
                Key = {Name, Vsn},
                Value = #{node => Node, status => plugin_status(Plugin)},
                SubAcc#{Key => [Value | maps:get(Key, Acc, [])]}
            end,
            Acc,
            Plugins
        ),
    aggregate_status(List, NewAcc).

format_plugin_avsc_and_i18n(NameVsn) ->
    case emqx_release:edition() of
        ee ->
            #{
                avsc => or_null(emqx_plugins:plugin_schema(NameVsn)),
                i18n => or_null(emqx_plugins:plugin_i18n(NameVsn))
            };
        ce ->
            #{avsc => null, i18n => null}
    end.

or_null({ok, Value}) -> Value;
or_null(_) -> null.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(B) when is_binary(B) -> B.

% running_status: running loaded, stopped
%% config_status: not_configured disable enable
plugin_status(#{running_status := running}) -> running;
plugin_status(_) -> stopped.
