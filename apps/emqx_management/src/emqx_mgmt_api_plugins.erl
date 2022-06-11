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
-module(emqx_mgmt_api_plugins).

-behaviour(minirest_api).

-include_lib("kernel/include/file.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
%%-include_lib("emqx_plugins/include/emqx_plugins.hrl").

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
    update_boot_order/2
]).

-export([
    validate_name/1,
    get_plugins/0,
    install_package/2,
    delete_package/1,
    describe_package/1,
    ensure_action/2
]).

-define(NAME_RE, "^[A-Za-z]+[A-Za-z0-9-_.]*$").

namespace() -> "plugins".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

%% Don't change the path's order
paths() ->
    [
        "/plugins",
        "/plugins/:name",
        "/plugins/install",
        "/plugins/:name/:action",
        "/plugins/:name/move"
    ].

schema("/plugins") ->
    #{
        'operationId' => list_plugins,
        get => #{
            description =>
                "List all install plugins.</br>"
                "Plugins are launched in top-down order.</br>"
                "Using `POST /plugins/{name}/move` to change the boot order.",
            responses => #{
                200 => hoconsc:array(hoconsc:ref(plugin))
            }
        }
    };
schema("/plugins/install") ->
    #{
        'operationId' => upload_install,
        post => #{
            description =>
                "Install a plugin(plugin-vsn.tar.gz)."
                "Follow [emqx-plugin-template](https://github.com/emqx/emqx-plugin-template) "
                "to develop plugin.",
            'requestBody' => #{
                content => #{
                    'multipart/form-data' => #{
                        schema => #{
                            type => object,
                            properties => #{
                                plugin => #{type => string, format => binary}
                            }
                        },
                        encoding => #{plugin => #{'contentType' => 'application/gzip'}}
                    }
                }
            },
            responses => #{
                200 => <<"OK">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['UNEXPECTED_ERROR', 'ALREADY_INSTALLED', 'BAD_PLUGIN_INFO']
                )
            }
        }
    };
schema("/plugins/:name") ->
    #{
        'operationId' => plugin,
        get => #{
            description => "Describe a plugin according `release.json` and `README.md`.",
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:ref(plugin),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        },
        delete => #{
            description => "Uninstall a plugin package.",
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => <<"Uninstall successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        }
    };
schema("/plugins/:name/:action") ->
    #{
        'operationId' => update_plugin,
        put => #{
            description =>
                "start/stop a installed plugin.</br>"
                "- **start**: start the plugin.</br>"
                "- **stop**: stop the plugin.</br>",
            parameters => [
                hoconsc:ref(name),
                {action, hoconsc:mk(hoconsc:enum([start, stop]), #{desc => "Action", in => path})}
            ],
            responses => #{
                200 => <<"OK">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Plugin Not Found">>)
            }
        }
    };
schema("/plugins/:name/move") ->
    #{
        'operationId' => update_boot_order,
        post => #{
            description => "Setting the boot order of plugins.",
            parameters => [hoconsc:ref(name)],
            'requestBody' => move_request_body(),
            responses => #{200 => <<"OK">>}
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
                    "Install plugin status at runtime</br>"
                    "1. running: plugin is running.</br>"
                    "2. stopped: plugin is stopped.</br>"
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
                nomatch -> {error, "Name should be " ?NAME_RE};
                _ -> ok
            end;
        false ->
            {error, "Name Length must =< 256"}
    end.

%% API CallBack Begin
list_plugins(get, _) ->
    {Plugins, []} = emqx_mgmt_api_plugins_proto_v1:get_plugins(),
    {200, format_plugins(Plugins)}.

get_plugins() ->
    {node(), emqx_plugins:list()}.

upload_install(post, #{body := #{<<"plugin">> := Plugin}}) when is_map(Plugin) ->
    [{FileName, Bin}] = maps:to_list(maps:without([type], Plugin)),
    %% File bin is too large, we use rpc:multicall instead of cluster_rpc:multicall
    %% TODO what happens when a new node join in?
    %% emqx_plugins_monitor should copy plugins from other core node when boot-up.
    case emqx_plugins:describe(string:trim(FileName, trailing, ".tar.gz")) of
        {error, #{error := "bad_info_file", return := {enoent, _}}} ->
            case emqx_plugins:parse_name_vsn(FileName) of
                {ok, AppName, _Vsn} ->
                    AppDir = filename:join(emqx_plugins:install_dir(), AppName),
                    case filelib:wildcard(AppDir ++ "*.tar.gz") of
                        [] ->
                            do_install_package(FileName, Bin);
                        OtherVsn ->
                            {400, #{
                                code => 'ALREADY_INSTALLED',
                                message => iolist_to_binary(
                                    io_lib:format(
                                        "~p already installed",
                                        [OtherVsn]
                                    )
                                )
                            }}
                    end;
                {error, Reason} ->
                    {400, #{
                        code => 'BAD_PLUGIN_INFO',
                        message => iolist_to_binary([Reason, ":", FileName])
                    }}
            end;
        {ok, _} ->
            {400, #{
                code => 'ALREADY_INSTALLED',
                message => iolist_to_binary(io_lib:format("~p is already installed", [FileName]))
            }}
    end;
upload_install(post, #{}) ->
    {400, #{
        code => 'BAD_FORM_DATA',
        message =>
            <<"form-data should be `plugin=@packagename-vsn.tar.gz;type=application/x-gzip`">>
    }}.

do_install_package(FileName, Bin) ->
    %% TODO: handle bad nodes
    {[_ | _] = Res, []} = emqx_mgmt_api_plugins_proto_v1:install_package(FileName, Bin),
    %% TODO: handle non-OKs
    [] = lists:filter(fun(R) -> R =/= ok end, Res),
    {200}.

plugin(get, #{bindings := #{name := Name}}) ->
    {Plugins, _} = emqx_mgmt_api_plugins_proto_v1:describe_package(Name),
    case format_plugins(Plugins) of
        [Plugin] -> {200, Plugin};
        [] -> {404, #{code => 'NOT_FOUND', message => Name}}
    end;
plugin(delete, #{bindings := #{name := Name}}) ->
    Res = emqx_mgmt_api_plugins_proto_v1:delete_package(Name),
    return(204, Res).

update_plugin(put, #{bindings := #{name := Name, action := Action}}) ->
    Res = emqx_mgmt_api_plugins_proto_v1:ensure_action(Name, Action),
    return(204, Res).

update_boot_order(post, #{bindings := #{name := Name}, body := Body}) ->
    case parse_position(Body, Name) of
        {error, Reason} ->
            {400, #{code => 'BAD_POSITION', message => Reason}};
        Position ->
            case emqx_plugins:ensure_enabled(Name, Position) of
                ok ->
                    {200};
                {error, Reason} ->
                    {400, #{
                        code => 'MOVE_FAILED',
                        message => iolist_to_binary(io_lib:format("~p", [Reason]))
                    }}
            end
    end.

%% API CallBack End

%% For RPC upload_install/2
install_package(FileName, Bin) ->
    File = filename:join(emqx_plugins:install_dir(), FileName),
    ok = file:write_file(File, Bin),
    PackageName = string:trim(FileName, trailing, ".tar.gz"),
    emqx_plugins:ensure_installed(PackageName).

%% For RPC plugin get
describe_package(Name) ->
    Node = node(),
    case emqx_plugins:describe(Name) of
        {ok, Plugin} -> {Node, [Plugin]};
        _ -> {Node, []}
    end.

%% For RPC plugin delete
delete_package(Name) ->
    case emqx_plugins:ensure_stopped(Name) of
        ok ->
            _ = emqx_plugins:ensure_disabled(Name),
            _ = emqx_plugins:ensure_uninstalled(Name),
            _ = emqx_plugins:delete_package(Name),
            ok;
        Error ->
            Error
    end.

%% for RPC plugin update
ensure_action(Name, start) ->
    _ = emqx_plugins:ensure_enabled(Name),
    _ = emqx_plugins:ensure_started(Name),
    ok;
ensure_action(Name, stop) ->
    _ = emqx_plugins:ensure_stopped(Name),
    _ = emqx_plugins:ensure_disabled(Name),
    ok;
ensure_action(Name, restart) ->
    _ = emqx_plugins:ensure_enabled(Name),
    _ = emqx_plugins:restart(Name),
    ok.

return(Code, ok) ->
    {Code};
return(_, {error, Reason}) ->
    {400, #{code => 'PARAM_ERROR', message => iolist_to_binary(io_lib:format("~p", [Reason]))}}.

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

% running_status: running loaded, stopped
%% config_status: not_configured disable enable
plugin_status(#{running_status := running}) -> running;
plugin_status(_) -> stopped.
