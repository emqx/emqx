%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_plugins).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_plugins/include/emqx_plugins.hrl").
-include_lib("erlavro/include/erlavro.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    upload_plugin_config/2,
    download_plugin_config/2,
    plugin_schema/2,
    update_boot_order/2,
    sync_plugin/2
]).

-export([
    validate_name/1,
    validate_file_name/2,
    get_plugins/0,
    install_package/2,
    install_package_v4/2,
    delete_package/1,
    delete_package/2,
    describe_package/1,
    ensure_action/2,
    ensure_action/3,
    do_update_plugin_config/3,
    do_update_plugin_config_v4/2,
    ensure_existed/1,
    sync_plugin_cluster/2
]).
-export([scopes/0]).

-define(NAME_RE, "^[A-Za-z]+\\w*\\-[\\w.-]*$").
-define(TAGS, [<<"Plugins">>]).

-define(CONTENT_PLUGIN, plugin).

namespace() ->
    "plugins".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

%% Don't change the path's order
scopes() -> ?SCOPE_SYSTEM.

paths() ->
    [
        "/plugins",
        "/plugins/:name",
        "/plugins/install",
        "/plugins/:name/:action",
        "/plugins/:name/config",
        "/plugins/:name/config/download",
        "/plugins/:name/config/upload",
        "/plugins/:name/schema",
        "/plugins/:name/move",
        "/plugins/cluster_sync"
    ].

schema("/plugins") ->
    #{
        'operationId' => list_plugins,
        get => #{
            description => ?DESC("list_plugins_desc"),
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
            description => ?DESC("install_plugin_desc"),
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
                204 => ?DESC("install_success"),
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
            description => ?DESC("get_plugin_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:ref(plugin),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found"))
            }
        },
        delete => #{
            description => ?DESC("delete_plugin_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => ?DESC("uninstall_success"),
                400 => emqx_dashboard_swagger:error_codes(['PARAM_ERROR'], ?DESC("bad_parameter")),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found")),
                500 => emqx_dashboard_swagger:error_codes(
                    ['INTERNAL_ERROR'], ?DESC("internal_error")
                )
            }
        }
    };
schema("/plugins/:name/:action") ->
    #{
        'operationId' => update_plugin,
        put => #{
            description => ?DESC("trigger_action_desc"),
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(name),
                {action,
                    hoconsc:mk(hoconsc:enum([start, stop]), #{desc => ?DESC("action"), in => path})}
            ],
            responses => #{
                204 => ?DESC("trigger_success"),
                400 => emqx_dashboard_swagger:error_codes(
                    ['PARAM_ERROR', 'BAD_CONFIG'], ?DESC("bad_parameter")
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found")),
                500 => emqx_dashboard_swagger:error_codes(
                    ['INTERNAL_ERROR'], ?DESC("internal_error")
                )
            }
        }
    };
schema("/plugins/:name/config") ->
    #{
        'operationId' => plugin_config,
        get => #{
            description => ?DESC("get_plugin_config_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                %% avro data, json encoded
                200 => hoconsc:mk(binary()),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG'], ?DESC("plugin_config_not_found")
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found"))
            }
        },
        put => #{
            description => ?DESC("update_plugin_config_desc"),
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
                204 => ?DESC("config_updated"),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG', 'UNEXPECTED_ERROR'], ?DESC("update_config_failed")
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found")),
                500 => emqx_dashboard_swagger:error_codes(
                    ['INTERNAL_ERROR'], ?DESC("internal_error")
                )
            }
        }
    };
schema("/plugins/:name/config/download") ->
    #{
        'operationId' => download_plugin_config,
        get => #{
            description => ?DESC("download_plugin_config_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:mk(binary()),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG'], ?DESC("plugin_config_not_found")
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found"))
            }
        }
    };
schema("/plugins/:name/config/upload") ->
    #{
        'operationId' => upload_plugin_config,
        post => #{
            description => ?DESC("upload_plugin_config_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => #{
                content => #{
                    'multipart/form-data' => #{
                        schema => #{
                            type => object,
                            properties => #{
                                config => #{type => string, format => binary}
                            }
                        },
                        encoding => #{config => #{'contentType' => 'application/json'}}
                    }
                }
            },
            responses => #{
                204 => ?DESC("config_updated"),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_CONFIG', 'UNEXPECTED_ERROR'], ?DESC("update_config_failed")
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found")),
                500 => emqx_dashboard_swagger:error_codes(
                    ['INTERNAL_ERROR'], ?DESC("internal_error")
                )
            }
        }
    };
schema("/plugins/:name/schema") ->
    #{
        'operationId' => plugin_schema,
        get => #{
            description => ?DESC("get_plugin_schema_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                %% avro schema and i18n json object
                200 => hoconsc:mk(binary()),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND', 'FILE_NOT_EXISTED'],
                    ?DESC("plugin_not_found")
                )
            }
        }
    };
schema("/plugins/:name/move") ->
    #{
        'operationId' => update_boot_order,
        post => #{
            description => ?DESC("move_plugin_desc"),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => move_request_body(),
            responses => #{
                204 => ?DESC("boot_order_changed"),
                400 => emqx_dashboard_swagger:error_codes(['MOVE_FAILED'], ?DESC("move_failed")),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("plugin_not_found"))
            }
        }
    };
schema("/plugins/cluster_sync") ->
    #{
        'operationId' => sync_plugin,
        post => #{
            description => ?DESC("sync_plugin_desc"),
            tags => ?TAGS,
            'requestBody' => sync_request_body(),
            responses => #{
                204 => ?DESC("sync_success"),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_PLUGIN_INFO'], ?DESC("bad_plugin_info")
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC("plugin_not_found")
                )
            }
        }
    }.

fields(plugin) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("plugin_name"),
                    validator => fun ?MODULE:validate_name/1,
                    required => true,
                    example => "emqx_plugin_template-5.0-rc.1"
                }
            )},
        {author,
            hoconsc:mk(list(string()), #{
                desc => ?DESC("plugin_author"), example => [<<"EMQX Team">>]
            })},
        {builder, hoconsc:ref(?MODULE, builder)},
        {built_on_otp_release,
            hoconsc:mk(string(), #{desc => ?DESC("built_on_otp_release"), example => "24"})},
        {compatibility,
            hoconsc:mk(map(), #{
                desc => ?DESC("compatibility"), example => #{<<"emqx">> => <<"~>5.0">>}
            })},
        {git_commit_or_build_date,
            hoconsc:mk(string(), #{
                example => "2021-12-25",
                desc => ?DESC("git_commit_or_build_date")
            })},
        {functionality,
            hoconsc:mk(hoconsc:array(string()), #{
                desc => ?DESC("functionality"), example => [<<"Demo">>]
            })},
        {git_ref,
            hoconsc:mk(string(), #{
                desc => ?DESC("git_ref"), example => "ddab50fafeed6b1faea70fc9ffd8c700d7e26ec1"
            })},
        {metadata_vsn, hoconsc:mk(string(), #{desc => ?DESC("metadata_vsn"), example => "0.1.0"})},
        {rel_vsn,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("rel_vsn"),
                    required => true,
                    example => <<"5.0-rc.1">>
                }
            )},
        {rel_apps,
            hoconsc:mk(
                hoconsc:array(binary()),
                #{
                    desc => ?DESC("rel_apps"),
                    required => true,
                    example => [<<"emqx_plugin_template-5.0.0">>, <<"map_sets-1.1.0">>]
                }
            )},
        {repo,
            hoconsc:mk(string(), #{
                desc => ?DESC("repo"), example => "https://github.com/emqx/emqx-plugin-template"
            })},
        {description,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("description"),
                    required => true,
                    example => "This is an demo plugin description"
                }
            )},
        {running_status,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(running_status)),
                #{desc => ?DESC("running_status"), required => true}
            )},
        {readme,
            hoconsc:mk(binary(), #{
                example => "This is an demo plugin.",
                desc => ?DESC("readme"),
                required => false
            })},
        {health_status, hoconsc:ref(?MODULE, health_status)}
    ];
fields(health_status) ->
    [
        {status,
            hoconsc:mk(hoconsc:enum([ok, error]), #{desc => ?DESC("status"), example => error})},
        {message,
            hoconsc:mk(binary(), #{
                desc => ?DESC("message"), example => <<"Port unavailable: 3306">>
            })}
    ];
fields(name) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("plugin_name"),
                    example => "emqx_plugin_template-5.0-rc.1",
                    in => path,
                    validator => fun ?MODULE:validate_name/1
                }
            )}
    ];
fields(builder) ->
    [
        {contact,
            hoconsc:mk(string(), #{
                desc => ?DESC("plugin_builder"), example => "emqx-support@emqx.io"
            })},
        {name, hoconsc:mk(string(), #{desc => ?DESC("plugin_builder"), example => "EMQX Team"})},
        {website,
            hoconsc:mk(string(), #{desc => ?DESC("plugin_builder"), example => "www.emqx.com"})}
    ];
fields(position) ->
    [
        {position,
            hoconsc:mk(
                hoconsc:union([front, rear, binary()]),
                #{
                    desc => ?DESC("position"),
                    required => false
                }
            )}
    ];
fields(sync_request) ->
    [
        {name,
            hoconsc:mk(string(), #{
                desc => ?DESC("sync_request"),
                example => "emqx_plugin_demo-5.1-rc.2",
                required => true
            })}
    ];
fields(running_status) ->
    [
        {node, hoconsc:mk(string(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})},
        {status,
            hoconsc:mk(hoconsc:enum([running, stopped]), #{
                desc => ?DESC("status")
            })}
    ].

move_request_body() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:ref(?MODULE, position),
        #{
            move_to_front => #{
                value => #{position => <<"front">>}
            },
            move_to_rear => #{
                value => #{position => <<"rear">>}
            },
            move_to_before => #{
                value => #{position => <<"before:emqx_plugin_demo-5.1-rc.2">>}
            },
            move_to_after => #{
                value => #{position => <<"after:emqx_plugin_demo-5.1-rc.2">>}
            }
        }
    ).

sync_request_body() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:ref(?MODULE, sync_request),
        #{
            sync_from_node => #{
                value => #{name => <<"emqx_plugin_demo-5.1-rc.2">>}
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
    {Plugins, BadNodes} = emqx_mgmt_api_plugins_proto_v4:get_plugins(Nodes),
    BadNodes =/= [] andalso
        ?SLOG(warning, #{msg => "get_plugins_rpc_failed", bad_nodes => BadNodes}),
    {200, format_plugins(drop_bad_plugin_results(Plugins))}.

get_plugins() ->
    Plugins = emqx_plugins:list(?normal, #{health_check => true}),
    {node(), lists:filter(fun api_visible_plugin/1, Plugins)}.

upload_install(post, #{name := NameVsn, bin := Bin}) ->
    do_upload_install(NameVsn, Bin);
upload_install(post, #{}) ->
    {400, #{
        code => 'BAD_FORM_DATA',
        message =>
            <<"form-data should be `plugin=@packagename-vsn.tar.gz;type=application/x-gzip`">>
    }}.

do_upload_install(NameVsn, Bin) ->
    case describe_api_plugin(NameVsn, #{}) of
        {error, #{msg := "bad_info_file", reason := {enoent, _Path}}} ->
            case emqx_plugins:is_package_present(NameVsn) of
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
        {error, stale_plugin} ->
            install_package_on_nodes(NameVsn, Bin);
        {ok, _} ->
            already_installed(NameVsn)
    end.

already_installed(NameVsn) ->
    {400, #{
        code => 'ALREADY_INSTALLED',
        message => iolist_to_binary(io_lib:format("~p is already installed", [NameVsn]))
    }}.

install_package_on_nodes(NameVsn, Bin) ->
    case emqx_plugins:is_allowed_installation(NameVsn, Bin) of
        ok ->
            Result = do_install_package_on_nodes(NameVsn, Bin),
            ok = forget_allow_after_install(NameVsn, Result),
            Result;
        {error, not_allowed} ->
            Msg = iolist_to_binary([
                <<"Package is not allowed installation;">>,
                <<" first allow it to be installed by running:">>,
                <<" `emqx ctl plugins allow ">>,
                NameVsn,
                <<"`">>
            ]),
            {403, #{code => 'FORBIDDEN', message => Msg}};
        {error, sha256_mismatch} ->
            Msg = iolist_to_binary([
                <<"Package sha256 does not match the value bound by `emqx ctl plugins allow ">>,
                NameVsn,
                <<" sha256:...`">>
            ]),
            {403, #{code => 'FORBIDDEN', message => Msg}}
    end.

%% On a successful HTTP install, immediately revoke the cluster-wide allow
%% entry so the same grant cannot be reused for a subsequent (potentially
%% different) upload. On failure, leave the entry in place so the operator
%% can retry without having to re-issue `emqx ctl plugins allow'.
forget_allow_after_install(NameVsn, {204}) ->
    Nodes = emqx:running_nodes(),
    _ = emqx_plugins_proto_v3:disallow_installation(Nodes, NameVsn),
    ok;
forget_allow_after_install(_NameVsn, _Other) ->
    ok.

do_install_package_on_nodes(NameVsn, Bin) ->
    Nodes = emqx:running_nodes(),
    {Responses, BadNodes} =
        emqx_mgmt_api_plugins_proto_v4:install_package(Nodes, NameVsn, Bin),
    GoodNodes = Nodes -- BadNodes,
    NodeErrors = [
        {Node, Response}
     || {Node, Response} <- lists:zip(GoodNodes, Responses),
        Response =/= ok
    ],
    case {NodeErrors, BadNodes} of
        {[], []} ->
            {204};
        {NodeErrors, []} when NodeErrors =/= [] ->
            case lists:any(fun({_Node, Error}) -> is_rpc_error(Error) end, NodeErrors) of
                true ->
                    ?SLOG(error, #{
                        msg => "plugin_install_failed",
                        node_errors => NodeErrors
                    }),
                    {500, #{
                        code => 'INTERNAL_ERROR',
                        message => format_node_errors(NodeErrors)
                    }};
                false ->
                    ?SLOG(warning, #{
                        msg => "invalid_plugin_package",
                        node_errors => NodeErrors
                    }),
                    {400, #{
                        code => 'BAD_PLUGIN_INFO',
                        message => format_node_errors(NodeErrors)
                    }}
            end;
        {NodeErrors, BadNodes} ->
            ?SLOG(error, #{
                msg => "plugin_install_failed",
                node_errors => NodeErrors,
                bad_nodes => BadNodes
            }),
            {500, #{
                code => 'INTERNAL_ERROR',
                message => format_node_errors(
                    NodeErrors ++ [{Node, {badnode, Node}} || Node <- BadNodes]
                )
            }}
    end.

is_rpc_error({badrpc, _}) -> true;
is_rpc_error(_) -> false.

plugin(get, #{bindings := #{name := NameVsn}}) ->
    Nodes = emqx:running_nodes(),
    {Plugins, _} = emqx_mgmt_api_plugins_proto_v4:describe_package(Nodes, NameVsn),
    case format_plugins(drop_bad_plugin_results(Plugins)) of
        [Plugin] -> {200, Plugin};
        [] -> {404, #{code => 'NOT_FOUND', message => NameVsn}}
    end;
plugin(delete, #{bindings := #{name := NameVsn}}) ->
    case api_visible_on_any_node(NameVsn) of
        true ->
            Res = emqx_mgmt_api_plugins_proto_v4:delete_package(NameVsn),
            operation_response(delete, Res);
        false ->
            {404, plugin_not_found_msg()}
    end.

update_plugin(put, #{bindings := #{name := NameVsn, action := Action}}) ->
    case api_visible_on_any_node(NameVsn) of
        true ->
            Res = ensure_cluster_action(NameVsn, Action),
            operation_response(Action, Res);
        false ->
            {404, plugin_not_found_msg()}
    end.

plugin_config(get, #{bindings := #{name := NameVsn}}) ->
    get_plugin_config(NameVsn);
plugin_config(put, #{bindings := #{name := NameVsn}, body := Config}) ->
    put_plugin_config(NameVsn, Config).

upload_plugin_config(post, #{
    bindings := #{name := NameVsn}, body := #{<<"config">> := #{type := _} = ConfigUpload}
}) ->
    [{_FileName, ConfigBin}] = maps:to_list(maps:without([type], ConfigUpload)),
    put_plugin_config(NameVsn, ConfigBin).

download_plugin_config(get, #{bindings := #{name := NameVsn}}) ->
    case get_plugin_config(NameVsn) of
        {200, Headers0, Config} ->
            Headers = Headers0#{
                <<"content-disposition">> =>
                    <<"attachment; filename=\"", NameVsn/binary, ".json\"">>
            },
            {200, Headers, Config};
        FailureResponse ->
            FailureResponse
    end.

get_plugin_config(NameVsn) ->
    case describe_api_plugin(NameVsn, #{}) of
        {ok, _} ->
            case emqx_plugins:get_config(NameVsn, ?plugin_conf_not_found) of
                Config when is_map(Config) ->
                    {200, #{<<"content-type">> => <<"'application/json'">>}, Config};
                ?plugin_conf_not_found ->
                    {400, #{
                        code => 'BAD_CONFIG',
                        message => <<"Plugin Config Not Found">>
                    }}
            end;
        _ ->
            {404, plugin_not_found_msg()}
    end.

put_plugin_config(NameVsn, Config) ->
    Nodes = emqx:running_nodes(),
    case describe_api_plugin(NameVsn, #{}) of
        {ok, _} ->
            case emqx_plugins:decode_plugin_config_map(NameVsn, Config) of
                {ok, ?plugin_without_config_schema} ->
                    %% no plugin avro schema, just put the json map as-is
                    Res = emqx_mgmt_api_plugins_proto_v4:update_plugin_config(
                        Nodes, NameVsn, Config
                    ),
                    return_config_update_result(Res);
                {ok, _AvroValue} ->
                    %% cluster call with config in map (binary key-value)
                    Res = emqx_mgmt_api_plugins_proto_v4:update_plugin_config(
                        Nodes, NameVsn, Config
                    ),
                    return_config_update_result(Res);
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
    case describe_api_plugin(NameVsn, #{}) of
        {ok, _Plugin} ->
            {200, format_plugin_avsc_and_i18n(NameVsn)};
        _ ->
            {404, plugin_not_found_msg()}
    end.

update_boot_order(post, #{bindings := #{name := Name}, body := Body}) ->
    case describe_api_plugin(Name, #{}) of
        {ok, _Plugin} ->
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
            end;
        _ ->
            {404, plugin_not_found_msg()}
    end.

sync_plugin(post, #{body := Body}) ->
    case parse_sync_plugin_name(Body) of
        {ok, NameVsn} ->
            ?SLOG(debug, #{
                msg => "sync_plugin_to_cluster",
                keep_namevsn => NameVsn
            }),
            case describe_api_plugin(NameVsn, #{}) of
                {ok, _Plugin} ->
                    do_sync_plugin(NameVsn);
                {error, stale_plugin} ->
                    {404, plugin_not_found_msg()};
                _ ->
                    do_sync_plugin(NameVsn)
            end;
        {error, {plugin_error, Reason}} ->
            {400, #{
                code => 'BAD_PLUGIN_INFO',
                message => Reason
            }}
    end.

do_sync_plugin(NameVsn) ->
    case ensure_existed(NameVsn) of
        ok ->
            case
                emqx_mgmt_api_plugins_proto_v4:sync_plugin_cluster(
                    emqx:running_nodes(), node(), NameVsn
                )
            of
                {_Res, []} -> {204};
                {_Res, BadNodes} -> {400, plugin_sync_failed_msg(BadNodes)}
            end;
        {error, {plugin_error, _Reason}} ->
            {404, plugin_not_found_msg()}
    end.

%% API CallBack End

%% For RPC upload_install/2
install_package(FileName, Bin) ->
    NameVsn = string:trim(FileName, trailing, ".tar.gz"),
    install_package_v4(NameVsn, Bin).

install_package_v4(NameVsn, Bin) ->
    ok = emqx_plugins:write_package(NameVsn, Bin),
    case emqx_plugins:ensure_installed(NameVsn, ?fresh_install) of
        {error, #{reason := plugin_not_found}} = NotFound ->
            NotFound;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "failed_to_install_plugin",
                reason => Reason
            }),
            _ = emqx_plugins:delete_package(NameVsn),
            Error;
        Result ->
            Result
    end.

%% For RPC plugin get
describe_package(NameVsn) ->
    Node = node(),
    case describe_api_plugin(NameVsn) of
        {ok, Plugin} -> {Node, [Plugin]};
        _ -> {Node, []}
    end.

%% Tip: Don't delete delete_package/1, use before v571 cluster_rpc
delete_package(NameVsn) ->
    delete_package(NameVsn, #{}).

%% For RPC plugin delete
delete_package(NameVsn, _Opts) ->
    emqx_plugins:safe_delete_package(NameVsn).

%% Tip: Don't delete ensure_action/2, use before v571 cluster_rpc
ensure_action(Name, Action) ->
    ensure_action(Name, Action, #{}).

ensure_action(Name, start, _Opts) ->
    case emqx_plugins:ensure_started(Name) of
        ok -> emqx_plugins:ensure_enabled(Name);
        {error, _} = Error -> Error
    end;
ensure_action(Name, stop, _Opts) ->
    case emqx_plugins:ensure_stopped(Name) of
        ok -> emqx_plugins:ensure_disabled(Name);
        {error, _} = Error -> Error
    end;
ensure_action(Name, restart, _Opts) ->
    case emqx_plugins:restart(Name) of
        ok -> emqx_plugins:ensure_enabled(Name);
        {error, _} = Error -> Error
    end.

ensure_cluster_action(Name, stop) ->
    emqx_mgmt_api_plugins_proto_v4:ensure_action(Name, stop);
ensure_cluster_action(Name, start) ->
    Nodes = emqx:running_nodes(),
    ensure_start_packages_on_nodes(Nodes, Name).

ensure_start_packages_on_nodes(Nodes, Name) ->
    {Responses, BadNodes} =
        emqx_mgmt_api_plugins_proto_v5:ensure_start_package(Nodes, Name),
    GoodNodes = Nodes -- BadNodes,
    NodeErrors = [
        {Node, Response}
     || {Node, Response} <- lists:zip(GoodNodes, Responses),
        Response =/= ok
    ],
    case {lists:any(fun is_node_rpc_error/1, NodeErrors), BadNodes, NodeErrors} of
        {false, [], []} ->
            validate_and_start_on_nodes(Nodes, Name);
        {false, [], _} ->
            {error, {plugin_start_failed, NodeErrors}};
        _ ->
            {error, {plugin_start_unavailable, package, NodeErrors, BadNodes}}
    end.

%% Start validation is deliberately side-effect free, but it is not an atomic
%% transaction with the following starts.  Each node revalidates in
%% `emqx_plugins:ensure_started/1', and rollback is best effort if concurrent
%% lifecycle or configuration operations make these status hints stale.
validate_and_start_on_nodes(Nodes, Name) ->
    {Responses, BadNodes} = emqx_mgmt_api_plugins_proto_v5:validate_start(Nodes, Name),
    GoodNodes = Nodes -- BadNodes,
    NodeResponses = lists:zip(GoodNodes, Responses),
    NodeErrors = [
        {Node, Response}
     || {Node, Response} <- NodeResponses,
        not is_start_validation_result(Response)
    ],
    case {lists:any(fun is_node_rpc_error/1, NodeErrors), BadNodes, NodeErrors} of
        {false, [], []} ->
            NodeStates = [
                {Node, RunningStatus}
             || {Node, {ok, RunningStatus}} <- NodeResponses
            ],
            start_on_nodes(Name, NodeStates);
        {false, [], _} ->
            {error, {plugin_start_preflight_failed, NodeErrors}};
        _ ->
            {error, {plugin_start_unavailable, validation, NodeErrors, BadNodes}}
    end.

start_on_nodes(Name, NodeStates) ->
    Nodes = [Node || {Node, _RunningStatus} <- NodeStates],
    {Responses, BadNodes} = emqx_mgmt_api_plugins_proto_v5:ensure_started(Nodes, Name),
    GoodNodes = Nodes -- BadNodes,
    Errors = [
        {Node, Response}
     || {Node, Response} <- lists:zip(GoodNodes, Responses),
        Response =/= ok
    ],
    case {lists:any(fun is_node_rpc_error/1, Errors), BadNodes, Errors} of
        {false, [], []} ->
            enable_if_membership_unchanged(Name, NodeStates);
        {false, [], _} ->
            rollback_after_start_error(Name, NodeStates, {plugin_start_failed, Errors});
        _ ->
            rollback_after_start_error(
                Name,
                NodeStates,
                {plugin_start_unavailable, start, Errors, BadNodes}
            )
    end.

enable_if_membership_unchanged(Name, NodeStates) ->
    ValidatedNodes = lists:sort([Node || {Node, _} <- NodeStates]),
    CurrentNodes = lists:sort(emqx:running_nodes()),
    case CurrentNodes =:= ValidatedNodes of
        true ->
            case emqx_plugins:ensure_enabled(Name, no_move, global) of
                ok ->
                    ok;
                {error, Reason} ->
                    rollback_after_start_error(
                        Name,
                        NodeStates,
                        {plugin_start_enable_failed, Reason}
                    )
            end;
        false ->
            rollback_after_start_error(
                Name,
                NodeStates,
                {plugin_start_membership_changed, ValidatedNodes, CurrentNodes}
            )
    end.

rollback_after_start_error(Name, NodeStates, Cause) ->
    case rollback_starts(Name, NodeStates) of
        ok ->
            {error, Cause};
        {error, RollbackErrors} ->
            {error, {plugin_start_rollback_failed, Cause, RollbackErrors}}
    end.

rollback_starts(Name, NodeStates) ->
    Nodes = [Node || {Node, not_running} <- NodeStates],
    {Responses, BadNodes} = emqx_mgmt_api_plugins_proto_v5:ensure_stopped(Nodes, Name),
    GoodNodes = Nodes -- BadNodes,
    Errors =
        [
            {Node, Response}
         || {Node, Response} <- lists:zip(GoodNodes, Responses),
            Response =/= ok
        ] ++
            [{Node, {badnode, Node}} || Node <- BadNodes],
    case Errors of
        [] -> ok;
        [_ | _] -> {error, Errors}
    end.

is_start_validation_result({ok, RunningStatus}) when
    RunningStatus =:= running; RunningStatus =:= not_running
->
    true;
is_start_validation_result(_) ->
    false.

is_node_rpc_error({_Node, {badrpc, _}}) -> true;
is_node_rpc_error({_Node, _}) -> false.

%% for RPC plugin avro encoded config update
-spec do_update_plugin_config(name_vsn(), map() | binary(), any()) ->
    ok.
do_update_plugin_config(NameVsn, AvroJsonMap, _AvroValue) ->
    case do_update_plugin_config_v4(NameVsn, AvroJsonMap) of
        ok -> ok;
        {error, Reason} -> error(Reason)
    end.

-spec do_update_plugin_config_v4(name_vsn(), map() | binary()) ->
    ok | {error, term()}.
do_update_plugin_config_v4(NameVsn, AvroJsonMap) when is_binary(AvroJsonMap) ->
    do_update_plugin_config_v4(NameVsn, emqx_utils_json:decode(AvroJsonMap));
do_update_plugin_config_v4(NameVsn, AvroJsonMap) ->
    emqx_plugins:update_config(NameVsn, AvroJsonMap).

%% for RPC plugin ensure existed
-spec ensure_existed(name_vsn()) -> ok | {error, term()}.
ensure_existed(NameVsn) ->
    case emqx_plugins:ensure_installed(NameVsn) of
        ok -> ok;
        {error, _} -> {error, {plugin_error, <<"Plugin Not Found">>}}
    end.

%% for RPC plugin sync
-spec sync_plugin_cluster(node(), name_vsn()) -> ok.
sync_plugin_cluster(Node, NameVsn) when Node =:= node() ->
    _ = emqx_plugins:purge_other_versions(NameVsn),
    ok;
sync_plugin_cluster(Node, NameVsn) ->
    _ = emqx_plugins:purge_other_versions(NameVsn),
    emqx_plugins:get_package_from_node(Node, NameVsn).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

operation_response(_Operation, ok) ->
    {204};
operation_response(start, {error, {plugin_start_preflight_failed, NodeErrors}}) ->
    case classify_start_preflight_errors(NodeErrors) of
        invalid_config ->
            {400, #{
                code => 'BAD_CONFIG',
                message => format_node_errors(NodeErrors)
            }};
        conflicting_version ->
            {400, #{
                code => 'PARAM_ERROR',
                message => format_node_errors(NodeErrors)
            }};
        mixed_client_errors ->
            {400, #{
                code => 'PARAM_ERROR',
                message => format_node_errors(NodeErrors)
            }};
        internal_error ->
            internal_error_response(
                "plugin_start_preflight_failed",
                #{node_errors => NodeErrors},
                format_node_errors(NodeErrors)
            )
    end;
operation_response(
    start,
    {error, {plugin_start_unavailable, Phase, NodeErrors, BadNodes}}
) ->
    internal_error_response(
        "plugin_start_unavailable",
        #{phase => Phase, node_errors => NodeErrors, bad_nodes => BadNodes},
        format_node_errors(NodeErrors ++ [{Node, {badnode, Node}} || Node <- BadNodes])
    );
operation_response(
    start,
    {error, {plugin_start_membership_changed, ValidatedNodes, CurrentNodes}}
) ->
    internal_error_response(
        "plugin_start_membership_changed",
        #{validated_nodes => ValidatedNodes, current_nodes => CurrentNodes},
        <<"Cluster membership changed during plugin start. Retry when the cluster is stable.">>
    );
operation_response(start, {error, {plugin_start_rollback_failed, Cause, RollbackErrors}}) ->
    internal_error_response(
        "plugin_start_rollback_failed",
        #{cause => Cause, rollback_errors => RollbackErrors},
        <<
            "Plugin start failed and rollback was incomplete. "
            "Check server logs and plugin status on all nodes."
        >>
    );
operation_response(start, {error, {plugin_start_enable_failed, Reason}}) ->
    internal_error_response(
        "plugin_start_enable_failed",
        #{reason => Reason},
        <<
            "Plugin start was rolled back because enabling it in the cluster configuration failed. "
            "Check server logs for details."
        >>
    );
operation_response(start, {error, {plugin_start_failed, NodeErrors}}) ->
    internal_error_response(
        "plugin_start_failed",
        #{node_errors => NodeErrors},
        format_node_errors(NodeErrors)
    );
operation_response(Operation, {error, #{reason := {enoent, Path} = Reason} = Error}) ->
    ?SLOG(warning, #{
        msg => "plugin_resource_not_found",
        operation => Operation,
        path => Path,
        reason => Reason,
        error => Error
    }),
    {404, #{
        code => 'NOT_FOUND',
        message =>
            <<
                "plugin_resource_not_found: A required plugin resource was not found. "
                "Check server logs for details."
            >>
    }};
operation_response(Operation, {error, Reason}) ->
    internal_error_response(
        operation_error_keyword(Operation),
        #{operation => Operation, reason => Reason},
        <<"Plugin operation failed. Check server logs for details.">>
    ).

internal_error_response(Keyword, LogFields, Details) ->
    ?SLOG(error, LogFields#{msg => Keyword}),
    {500, #{
        code => 'INTERNAL_ERROR',
        message => iolist_to_binary([Keyword, ": ", Details])
    }}.

operation_error_keyword(start) -> "plugin_start_failed";
operation_error_keyword(stop) -> "plugin_stop_failed";
operation_error_keyword(delete) -> "plugin_delete_failed".

return_config_update_result({Responses, BadNodes}) ->
    ResponseErrors = lists:filter(fun(Response) -> Response =/= ok end, Responses),
    NodeErrors = [{badnode, Node} || Node <- BadNodes],
    case {ResponseErrors, NodeErrors} of
        {[], []} ->
            {204};
        {ResponseErrors, []} ->
            {400, #{code => 'BAD_CONFIG', message => format_errors(ResponseErrors)}};
        {ResponseErrors, NodeErrors} ->
            internal_error_response(
                "plugin_config_update_failed",
                #{response_errors => ResponseErrors, bad_nodes => BadNodes},
                format_errors(ResponseErrors ++ NodeErrors)
            )
    end.

plugin_not_found_msg() ->
    #{
        code => 'NOT_FOUND',
        message => <<"Plugin Not Found">>
    }.

format_errors(Errors) ->
    Msgs = lists:map(fun format_error/1, Errors),
    iolist_to_binary(lists:join("; ", Msgs)).

format_error({error, Msg}) ->
    readable_error_msg(Msg);
format_error({badnode, Node}) ->
    iolist_to_binary(io_lib:format("node ~ts unavailable", [Node]));
format_error(Other) ->
    readable_error_msg(Other).

format_node_errors(NodeErrors) ->
    iolist_to_binary(
        lists:join("; ", [format_node_error(Node, Error) || {Node, Error} <- NodeErrors])
    ).

format_node_error(Node, {badnode, Node}) ->
    iolist_to_binary(["node ", atom_to_binary(Node), " unavailable"]);
format_node_error(Node, Error) ->
    iolist_to_binary([
        "node ",
        atom_to_binary(Node),
        ": ",
        format_error(Error)
    ]).

classify_start_preflight_errors(NodeErrors) ->
    Errors = [Error || {_Node, Error} <- NodeErrors],
    Kinds = lists:filtermap(fun validation_error_kind/1, Errors),
    case length(Kinds) =:= length(Errors) of
        false ->
            internal_error;
        true ->
            case lists:usort(Kinds) of
                [] -> internal_error;
                [invalid_config] -> invalid_config;
                [conflicting_version] -> conflicting_version;
                [_ | _] -> mixed_client_errors
            end
    end.

validation_error_kind({error, #{kind := Kind}}) when
    Kind =:= invalid_config;
    Kind =:= invalid_package;
    Kind =:= conflicting_version
->
    {true, Kind};
validation_error_kind(_) ->
    false.

readable_error_msg(#{
    msg := "invalid_plugin_config",
    name_vsn := NameVsn,
    reason := #{
        reason := invalid_type,
        path := Path,
        expected := Expected,
        actual := Actual
    }
}) ->
    iolist_to_binary([
        "invalid_plugin_config: Plugin ",
        NameVsn,
        " configuration is invalid at '",
        Path,
        "': expected ",
        Expected,
        ", got ",
        Actual,
        ". Fix the plugin configuration on this node and retry."
    ]);
readable_error_msg(#{
    msg := "invalid_plugin_config",
    name_vsn := NameVsn,
    reason := _Reason
}) ->
    iolist_to_binary([
        "invalid_plugin_config: Plugin ",
        NameVsn,
        " configuration is invalid. Fix the plugin configuration on this node and retry."
    ]);
readable_error_msg(#{
    msg := "invalid_plugin_config_schema",
    name_vsn := NameVsn,
    reason := _Reason
}) ->
    iolist_to_binary([
        "invalid_plugin_config_schema: Plugin ",
        NameVsn,
        " contains an invalid configuration schema. Rebuild or reinstall a corrected plugin "
        "package and retry."
    ]);
readable_error_msg(#{
    msg := "bad_plugin_app_file",
    path := _Path,
    reason := _Reason
}) ->
    <<
        "bad_plugin_app_file: Plugin package metadata is invalid or unreadable. "
        "Rebuild or reinstall a corrected plugin package and retry."
    >>;
readable_error_msg(#{
    msg := "plugin_app_version_mismatch",
    path := _Path,
    expected_vsn := ExpectedVsn,
    actual_vsn := ActualVsn
}) ->
    iolist_to_binary([
        "plugin_app_version_mismatch: Plugin package metadata declares application version ",
        emqx_utils:readable_error_msg(ActualVsn),
        ", but ",
        ExpectedVsn,
        " is required. Rebuild or reinstall the correct plugin package and retry."
    ]);
readable_error_msg(#{
    msg := "plugin_app_loaded_outside_package",
    name := AppName,
    expected_ebin := _ExpectedEbin,
    loaded_ebin := _LoadedEbin
}) ->
    iolist_to_binary([
        "plugin_app_loaded_outside_package: Plugin application ",
        atom_to_binary(AppName),
        " is already loaded outside this plugin package. Remove the conflicting code path or "
        "restart the node, then retry."
    ]);
readable_error_msg(#{
    msg := "bad_default_hocon_file",
    reason := _Reason
}) ->
    <<
        "bad_default_hocon_file: Plugin package default configuration is invalid or "
        "unreadable. Rebuild or reinstall a corrected plugin package and retry."
    >>;
readable_error_msg(#{
    reason := bad_schema,
    details := _Details
}) ->
    <<
        "invalid_plugin_config_schema: Plugin package configuration schema is invalid. "
        "Rebuild or reinstall a corrected plugin package and retry."
    >>;
readable_error_msg(#{
    msg := "conflicting_plugin_version_running",
    active_versions := ActiveVersions
}) ->
    iolist_to_binary([
        "conflicting_plugin_version_running: Another version of this plugin is running: ",
        lists:join(", ", ActiveVersions),
        ". Stop the active version and retry."
    ]);
readable_error_msg(#{
    reason := invalid_type,
    path := Path,
    expected := Expected,
    actual := Actual
}) ->
    iolist_to_binary([
        "invalid_type: Invalid type for field '",
        Path,
        "': expected ",
        Expected,
        ", got ",
        Actual
    ]);
readable_error_msg(#{
    reason := invalid_union_member,
    path := Path,
    expected := Expected,
    actual := Actual
}) ->
    iolist_to_binary([
        "invalid_union_member: Invalid union member for field '",
        Path,
        "': expected ",
        Expected,
        ", got ",
        Actual
    ]);
readable_error_msg(Msg) ->
    emqx_utils:readable_error_msg(Msg).

-ifdef(TEST).

update_plugin_schema_exposes_param_error_test() ->
    #{
        put := #{
            responses := #{
                400 := _
            }
        }
    } = schema("/plugins/:name/:action").

-endif.

plugin_sync_failed_msg(Nodes) ->
    #{
        code => 'BAD_PLUGIN_INFO',
        message => iolist_to_binary(
            io_lib:format(
                "Failed to sync plugin on nodes: ~p",
                [Nodes]
            )
        )
    }.

describe_api_plugin(NameVsn) ->
    describe_api_plugin(NameVsn, #{fill_readme => true, health_check => true}).

describe_api_plugin(NameVsn, Options) ->
    case emqx_plugins:describe(NameVsn, Options) of
        {ok, Plugin} ->
            case api_visible_plugin(Plugin) of
                true -> {ok, Plugin};
                false -> {error, stale_plugin}
            end;
        Error ->
            Error
    end.

api_visible_plugin(#{config_status := not_configured, running_status := RunningStatus} = Plugin) ->
    case RunningStatus of
        running ->
            true;
        _ ->
            emqx_plugins:log_unconfigured_plugin(Plugin),
            false
    end;
api_visible_plugin(_) ->
    true.

api_visible_on_any_node(NameVsn) ->
    Nodes = emqx:running_nodes(),
    {Plugins, _} = emqx_mgmt_api_plugins_proto_v4:describe_package(Nodes, NameVsn),
    format_plugins(drop_bad_plugin_results(Plugins)) =/= [].

%% A remote crash surfaces in the rpc:multicall result list as {badrpc, {'EXIT', _}}.
drop_bad_plugin_results(Results) ->
    {Good, Bad} = lists:partition(
        fun
            ({Node, Plugins}) when is_atom(Node), is_list(Plugins) -> true;
            (_) -> false
        end,
        Results
    ),
    Bad =/= [] andalso
        ?SLOG(warning, #{msg => "get_plugins_rpc_bad_results", results => Bad}),
    Good.

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

-spec parse_sync_plugin_name(map()) -> {ok, string()} | {error, term()}.
parse_sync_plugin_name(#{<<"name">> := Name}) ->
    parse_sync_plugin_name(Name);
parse_sync_plugin_name(Name) ->
    try emqx_plugins_utils:parse_name_vsn(Name) of
        {_AppName, _Vsn} ->
            {ok, binary_to_list(Name)}
    catch
        error:bad_name_vsn ->
            {error, {plugin_error, <<"Bad Plugin Name Vsn">>}}
    end.

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
    #{name := Name, rel_vsn := Vsn} = Plugin0,
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
                #{name := Name, rel_vsn := Vsn} = Plugin,
                Key = {Name, Vsn},
                Value0 = #{
                    node => Node,
                    status => plugin_status(Plugin)
                },
                Value = add_health_status(Value0, Plugin),
                SubAcc#{Key => [Value | maps:get(Key, Acc, [])]}
            end,
            Acc,
            Plugins
        ),
    aggregate_status(List, NewAcc).

-dialyzer({nowarn_function, format_plugin_avsc_and_i18n/1}).
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

% running_status: running loaded, stopped
%% config_status: not_configured disable enable
plugin_status(#{running_status := running}) -> running;
plugin_status(_) -> stopped.

add_health_status(StatusInfo, #{health_status := HealthStatus}) ->
    StatusInfo#{health_status => HealthStatus};
add_health_status(StatusInfo, _) ->
    StatusInfo.

-ifdef(TEST).

ensure_action_test_() ->
    {setup,
        fun() ->
            meck:new(emqx_plugins, [passthrough]),
            ok
        end,
        fun(_) ->
            meck:unload(emqx_plugins)
        end,
        [
            fun ensure_action_start_propagates_error_case/0,
            fun ensure_action_stop_propagates_error_case/0,
            fun ensure_action_restart_propagates_restart_error_case/0,
            fun ensure_action_restart_propagates_enable_error_case/0,
            fun ensure_action_success_case/0
        ]}.

ensure_action_start_propagates_error_case() ->
    meck:expect(emqx_plugins, ensure_started, fun(_Name) -> {error, start_failed} end),
    ?assertEqual({error, start_failed}, ensure_action(<<"demo-1.0.0">>, start, #{})).

ensure_action_stop_propagates_error_case() ->
    meck:expect(emqx_plugins, ensure_stopped, fun(_Name) -> {error, stop_failed} end),
    ?assertEqual({error, stop_failed}, ensure_action(<<"demo-1.0.0">>, stop, #{})).

ensure_action_restart_propagates_restart_error_case() ->
    meck:expect(emqx_plugins, restart, fun(_Name) -> {error, restart_failed} end),
    meck:expect(emqx_plugins, ensure_enabled, fun(_Name) -> ok end),
    ?assertEqual({error, restart_failed}, ensure_action(<<"demo-1.0.0">>, restart, #{})),
    ?assertNot(meck:called(emqx_plugins, ensure_enabled, ['_'])).

ensure_action_restart_propagates_enable_error_case() ->
    meck:expect(emqx_plugins, restart, fun(_Name) -> ok end),
    meck:expect(emqx_plugins, ensure_enabled, fun(_Name) -> {error, enable_failed} end),
    ?assertEqual({error, enable_failed}, ensure_action(<<"demo-1.0.0">>, restart, #{})).

ensure_action_success_case() ->
    meck:expect(emqx_plugins, ensure_started, fun(_Name) -> ok end),
    meck:expect(emqx_plugins, ensure_enabled, fun(_Name) -> ok end),
    meck:expect(emqx_plugins, ensure_stopped, fun(_Name) -> ok end),
    meck:expect(emqx_plugins, ensure_disabled, fun(_Name) -> ok end),
    meck:expect(emqx_plugins, restart, fun(_Name) -> ok end),
    ?assertEqual(ok, ensure_action(<<"demo-1.0.0">>, start, #{})),
    ?assertEqual(ok, ensure_action(<<"demo-1.0.0">>, stop, #{})),
    ?assertEqual(ok, ensure_action(<<"demo-1.0.0">>, restart, #{})).

-endif.
