%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_data_backup).

-feature(maybe_expr, enable).

-behaviour(minirest_api).

-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([
    data_export/2,
    data_import/2,
    data_files/2,
    data_file_by_name/2
]).

%% Smoke test
-export([
    check_desc/0
]).
-export([scopes/0]).

-define(TAGS, [<<"Data Backup">>]).
-define(BPAPI_NAME, emqx_mgmt_data_backup).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

scopes() -> ?SCOPE_SYSTEM.

paths() ->
    [
        "/data/export",
        "/data/import",
        "/data/files",
        "/data/files/:filename"
    ].

schema("/data/export") ->
    #{
        'operationId' => data_export,
        post => #{
            tags => ?TAGS,
            description => ?DESC("data_export"),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ?R_REF(export_request_body),
                export_request_example()
            ),
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?R_REF(backup_file_info),
                        backup_file_info_example()
                    ),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("invalid_table_sets")
                ),
                500 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("export_error")
                )
            }
        }
    };
schema("/data/import") ->
    #{
        'operationId' => data_import,
        post => #{
            tags => ?TAGS,
            description => ?DESC("data_import"),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ?R_REF(import_request_body),
                maps:with([node, filename], backup_file_info_example())
            ),

            responses => #{
                204 => <<"No Content">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("import_failed")
                )
            }
        }
    };
schema("/data/files") ->
    #{
        'operationId' => data_files,
        post => #{
            tags => ?TAGS,
            description => ?DESC("upload_backup_file"),
            'requestBody' => emqx_dashboard_swagger:file_schema(filename),
            responses => #{
                204 => <<"No Content">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("bad_backup_file")
                )
            }
        },
        get => #{
            tags => ?TAGS,
            desc => ?DESC("list_backup_files"),
            parameters => [
                ?R_REF(emqx_dashboard_swagger, page),
                ?R_REF(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?R_REF(files_response),
                        files_response_example()
                    )
            }
        }
    };
schema("/data/files/:filename") ->
    #{
        'operationId' => data_file_by_name,
        get => #{
            tags => ?TAGS,
            description => ?DESC("download_backup_file"),
            parameters => [
                field_filename(true, #{in => path}),
                field_node(false, #{in => query})
            ],
            responses => #{
                200 => ?HOCON(binary),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("bad_request")
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    [?NOT_FOUND], ?DESC("backup_file_not_found")
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            desc => ?DESC("delete_backup_file"),
            parameters => [
                field_filename(true, #{in => path}),
                field_node(false, #{in => query})
            ],
            responses => #{
                204 => <<"No Content">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST], ?DESC("bad_request")
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    [?NOT_FOUND], ?DESC("backup_file_not_found")
                )
            }
        }
    }.

fields(files_response) ->
    [
        {data, ?ARRAY(?R_REF(backup_file_info))},
        {meta, ?R_REF(emqx_dashboard_swagger, meta)}
    ];
fields(backup_file_info) ->
    [
        field_node(true),
        field_filename(true),
        {created_at,
            ?HOCON(binary(), #{
                desc => ?DESC("created_at"),
                required => true
            })}
    ];
fields(export_request_body) ->
    [
        {table_sets,
            hoconsc:mk(
                hoconsc:array(binary()),
                #{
                    required => false,
                    desc => ?DESC("table_sets")
                }
            )},
        {root_keys,
            hoconsc:mk(
                hoconsc:array(binary()),
                #{
                    required => false,
                    desc => ?DESC("root_keys")
                }
            )}
    ];
fields(import_request_body) ->
    [field_node(false), field_filename(true)];
fields(data_backup_file) ->
    [
        field_filename(true),
        {file,
            ?HOCON(binary(), #{
                desc => ?DESC("file_content"),
                required => true
            })}
    ].

field_node(IsRequired) ->
    field_node(IsRequired, #{}).

field_node(IsRequired, Meta) ->
    {node, ?HOCON(binary(), Meta#{desc => ?DESC("node_name"), required => IsRequired})}.

field_filename(IsRequired) ->
    field_filename(IsRequired, #{}).

field_filename(IsRequired, Meta) ->
    {filename,
        ?HOCON(binary(), Meta#{
            desc => ?DESC("filename"),
            required => IsRequired
        })}.

%%------------------------------------------------------------------------------
%% HTTP API Callbacks
%%------------------------------------------------------------------------------

data_export(post, #{body := Params0} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    Params1 = emqx_utils_maps:put_if(Params0, <<"namespace">>, Namespace, is_binary(Namespace)),
    Params = maybe_filter_export_params(Params1, auth_meta(Req)),
    maybe
        ok ?= emqx_mgmt_data_backup:validate_export_root_keys(Params),
        {ok, Opts} ?= emqx_mgmt_data_backup:parse_export_request(Params),
        {ok, #{filename := Filename} = File} ?= emqx_mgmt_data_backup:export(Opts),
        {200, File#{filename => filename:basename(Filename)}}
    else
        {error, {unknown_root_keys, UnknownKeys}} ->
            Msg = iolist_to_binary([
                <<"Invalid root keys: ">>,
                lists:join(<<", ">>, UnknownKeys)
            ]),
            {400, #{code => ?BAD_REQUEST, message => Msg}};
        {error, {bad_table_sets, InvalidSetNames}} ->
            Msg = iolist_to_binary([
                <<"Invalid table sets: ">>,
                lists:join(<<", ">>, InvalidSetNames)
            ]),
            {400, #{code => ?BAD_REQUEST, message => Msg}};
        {error, Reason} ->
            Msg = iolist_to_binary([
                <<"Error processing export: ">>,
                emqx_utils_conv:bin(Reason)
            ]),
            {500, #{code => 'INTERNAL_ERROR', message => Msg}}
    end.

data_import(post, #{body := #{<<"filename">> := Filename} = Body} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI_NAME, 2),
    case safe_parse_node(Body, Nodes) of
        {error, Msg} ->
            {400, #{code => ?BAD_REQUEST, message => Msg}};
        FileNode ->
            case check_no_sensitive_tables(Filename, auth_meta(Req)) of
                {forbidden, Sets} ->
                    {403, #{code => 'FORBIDDEN', message => import_refused_msg(Sets)}};
                {peek_error, Reason} ->
                    {400, #{
                        code => ?BAD_REQUEST,
                        message => emqx_mgmt_data_backup:format_error(Reason)
                    }};
                ok ->
                    do_data_import(FileNode, Filename, Namespace)
            end
    end.

do_data_import(FileNode, Filename, Namespace) ->
    CoreNode = core_node(FileNode),
    Opts = emqx_utils_maps:put_if(#{}, namespace, Namespace, is_binary(Namespace)),
    Res = emqx_mgmt_data_backup_proto_v2:import_file(
        CoreNode,
        FileNode,
        Filename,
        Opts,
        infinity
    ),
    case Res of
        {ok, #{db_errors := DbErrs, config_errors := ConfErrs}} ->
            case DbErrs =:= #{} andalso ConfErrs =:= #{} of
                true ->
                    {204};
                false ->
                    Msg = format_import_errors(DbErrs, ConfErrs),
                    {400, #{code => ?BAD_REQUEST, message => Msg}}
            end;
        {badrpc, Reason} ->
            {500, #{
                code => ?SERVICE_UNAVAILABLE(Reason),
                message => emqx_mgmt_data_backup:format_error(Reason)
            }};
        {error, Reason} ->
            {400, #{
                code => ?BAD_REQUEST, message => emqx_mgmt_data_backup:format_error(Reason)
            }}
    end.

format_import_errors(DbErrs, ConfErrs) ->
    DbErrs1 = emqx_mgmt_data_backup:format_db_errors(DbErrs),
    ConfErrs1 = emqx_mgmt_data_backup:format_conf_errors(ConfErrs),
    GlobalConfErrs = maps:get(?global_ns, ConfErrs1, <<"">>),
    Msg0 = ConfErrs1#{
        ?global_ns => [
            DbErrs1,
            GlobalConfErrs
        ]
    },
    Msg1 = maps:map(fun(_Ns, IOData) -> iolist_to_binary(IOData) end, Msg0),
    maps:filter(fun(_Ns, Text) -> Text /= <<"">> end, Msg1).

core_node(FileNode) ->
    case mria_rlog:role(FileNode) of
        core ->
            FileNode;
        replicant ->
            case mria_rlog:role() of
                core ->
                    node();
                replicant ->
                    mria_membership:coordinator()
            end
    end.

data_files(post, #{body := #{<<"filename">> := #{type := _} = File}}) ->
    [{Filename, FileContent} | _] = maps:to_list(maps:without([type], File)),
    case emqx_mgmt_data_backup:upload(Filename, FileContent) of
        ok ->
            {204};
        {error, Reason} ->
            {400, #{code => ?BAD_REQUEST, message => emqx_mgmt_data_backup:format_error(Reason)}}
    end;
data_files(post, #{body := _}) ->
    {400, #{code => ?BAD_REQUEST, message => ?DESC("missing_filename")}};
data_files(get, #{query_string := PageParams}) ->
    case emqx_mgmt_api:parse_pager_params(PageParams) of
        false ->
            {400, #{code => ?BAD_REQUEST, message => ?DESC("page_limit_invalid")}};
        #{page := Page, limit := Limit} = Pager ->
            {Count, HasNext, Data} = list_backup_files(Page, Limit),
            {200, #{data => Data, meta => Pager#{count => Count, hasnext => HasNext}}}
    end.

data_file_by_name(get, #{bindings := #{filename := Filename}, query_string := QS} = Req) ->
    AuthMeta = auth_meta(Req),
    case can_attempt_download(AuthMeta) of
        false ->
            {403, #{code => 'FORBIDDEN', message => download_forbidden_msg()}};
        true ->
            case safe_parse_node(QS) of
                {error, Msg} ->
                    {400, #{code => ?BAD_REQUEST, message => Msg}};
                Node ->
                    case can_download_backup(AuthMeta, Filename, Node) of
                        ok ->
                            handle_file_op_response(get_or_delete_file(get, Filename, Node));
                        {forbidden, Msg} ->
                            {403, #{code => ?FORBIDDEN, message => Msg}};
                        {error, Reason} ->
                            handle_file_op_response(Reason)
                    end
            end
    end;
data_file_by_name(delete, #{bindings := #{filename := Filename}, query_string := QS}) ->
    case safe_parse_node(QS) of
        {error, Msg} ->
            {400, #{code => ?BAD_REQUEST, message => Msg}};
        Node ->
            handle_file_op_response(get_or_delete_file(delete, Filename, Node))
    end.

handle_file_op_response({error, not_found}) ->
    {404, #{code => ?NOT_FOUND, message => emqx_mgmt_data_backup:format_error(not_found)}};
handle_file_op_response(ok) ->
    ?NO_CONTENT;
handle_file_op_response({ok, BinContents}) ->
    {200, #{<<"content-type">> => <<"application/octet-stream">>}, BinContents};
handle_file_op_response({error, Reason}) ->
    {400, #{code => ?BAD_REQUEST, message => emqx_mgmt_data_backup:format_error(Reason)}};
handle_file_op_response({badrpc, Reason}) ->
    {500, #{
        code => ?SERVICE_UNAVAILABLE(Reason),
        message => emqx_mgmt_data_backup:format_error(Reason)
    }}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

auth_meta(#{auth_meta := AuthMeta}) -> AuthMeta;
auth_meta(_) -> #{}.

%% The `dashboard_users' and `api_keys' table sets are governed at their
%% dedicated REST endpoints (`/users', `/api_key') by the
%% `user_management' and `api_key_management' scopes. A caller may only
%% back up or restore those tables through the data-backup endpoints when
%% it holds the same scopes; otherwise the sensitive table sets are
%% filtered on export and rejected on import. The predicate returns
%% `true' when this restriction applies to the caller.
%%
%%   * API-key callers: always restricted (API keys cannot hold the
%%     credential-management scopes at all).
%%   * Dashboard JWT callers: restricted unless they are a global
%%     administrator whose effective scope set includes both
%%     `user_management' and `api_key_management'.
%%   * Anything else: restricted (fail closed).
requires_sensitive_table_check(#{auth_type := api_key}) ->
    true;
requires_sensitive_table_check(#{auth_type := jwt_token} = AuthMeta) ->
    not has_credential_management_scopes(AuthMeta);
requires_sensitive_table_check(_) ->
    true.

has_credential_management_scopes(#{
    source := Source,
    actor := #{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}
}) ->
    Scopes = emqx_dashboard_admin:effective_scopes_of(Source),
    lists:member(?SCOPE_USER_MGMT, Scopes) andalso
        lists:member(?SCOPE_API_KEY_MGMT, Scopes);
has_credential_management_scopes(_) ->
    false.

%% Callers without the credential-management scopes must not export tables
%% that hold dashboard accounts or API keys themselves. Force-filter the
%% requested table sets accordingly.
maybe_filter_export_params(Params, AuthMeta) ->
    case requires_sensitive_table_check(AuthMeta) of
        true -> filter_sensitive_table_sets(Params);
        false -> Params
    end.

filter_sensitive_table_sets(Params) ->
    Sensitive = emqx_mgmt_data_backup:sensitive_table_set_names(),
    Allowed =
        case maps:find(<<"table_sets">>, Params) of
            {ok, Requested} -> Requested -- Sensitive;
            error -> emqx_mgmt_data_backup:all_table_set_names() -- Sensitive
        end,
    Params#{<<"table_sets">> => Allowed}.

%% Peek the local file. If the file lives on a different node (caller passed
%% `node' in the body), the local peek returns `{error, not_found}' and the
%% handler reports a 400 -- restricted callers must upload to the same node
%% where they import.
check_no_sensitive_tables(Filename, AuthMeta) ->
    case requires_sensitive_table_check(AuthMeta) of
        false ->
            ok;
        true ->
            case emqx_mgmt_data_backup:peek_sensitive_table_sets(Filename) of
                {ok, []} -> ok;
                {ok, Sets} -> {forbidden, Sets};
                {error, Reason} -> {peek_error, Reason}
            end
    end.

%% Stored backups may contain `dashboard_users' / `api_keys' mnesia tables
%% (salted password hashes, MFA / TOTP material, API-key records). Global
%% dashboard administrators may download any archive. API-key callers may only
%% download archives whose table list proves they do not contain those
%% sensitive records; fail closed if we cannot inspect the archive.
can_attempt_download(#{
    auth_type := jwt_token,
    actor := #{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}
}) ->
    true;
can_attempt_download(#{auth_type := api_key}) ->
    true;
can_attempt_download(_) ->
    false.

can_download_backup(
    #{auth_type := jwt_token, actor := #{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}},
    _Filename,
    _Node
) ->
    ok;
can_download_backup(#{auth_type := api_key}, Filename, Node) ->
    case emqx_mgmt_data_backup_proto_v2:peek_sensitive_table_sets(Node, Filename, infinity) of
        {ok, []} ->
            ok;
        {ok, Sets} ->
            {forbidden, api_key_download_forbidden_msg(Sets)};
        Error ->
            {error, Error}
    end;
can_download_backup(_, _Filename, _Node) ->
    {forbidden, download_forbidden_msg()}.

download_forbidden_msg() ->
    <<
        "Only the global administrator may download backup files. "
        "Backups may contain dashboard accounts and API key records."
    >>.

import_refused_msg(Sets) ->
    iolist_to_binary([
        <<
            "Import refused: backup contains tables that require the "
            "user_management and api_key_management scopes: "
        >>,
        lists:join(<<", ">>, Sets)
    ]).

api_key_download_forbidden_msg(Sets) ->
    iolist_to_binary([
        <<"API key download refused: backup contains restricted tables: ">>,
        lists:join(<<", ">>, Sets)
    ]).

get_or_delete_file(get, Filename, Node) ->
    emqx_mgmt_data_backup_proto_v1:read_file(Node, Filename, infinity);
get_or_delete_file(delete, Filename, Node) ->
    emqx_mgmt_data_backup_proto_v1:delete_file(Node, Filename, infinity).

safe_parse_node(BodyParams) ->
    Nodes = emqx:running_nodes(),
    safe_parse_node(BodyParams, Nodes).

safe_parse_node(#{<<"node">> := NodeBin}, Nodes) ->
    NodesBin = [erlang:atom_to_binary(N, utf8) || N <- Nodes],
    case lists:member(NodeBin, NodesBin) of
        true -> erlang:binary_to_atom(NodeBin, utf8);
        false -> {error, io_lib:format("Unknown node: ~s", [NodeBin])}
    end;
safe_parse_node(_, _) ->
    node().

list_backup_files(Page, Limit) ->
    Start = Page * Limit - Limit + 1,
    AllFiles = list_backup_files(),
    Count = length(AllFiles),
    HasNext = Start + Limit - 1 < Count,
    {Count, HasNext, lists:sublist(AllFiles, Start, Limit)}.

list_backup_files() ->
    Nodes = emqx:running_nodes(),
    Results = emqx_mgmt_data_backup_proto_v1:list_files(Nodes, 30_0000),
    NodeResults = lists:zip(Nodes, Results),
    {Successes, Failures} =
        lists:partition(
            fun({_Node, Result}) ->
                case Result of
                    {ok, _} -> true;
                    _ -> false
                end
            end,
            NodeResults
        ),
    case Failures of
        [] ->
            ok;
        [_ | _] ->
            ?SLOG(error, #{msg => "list_exported_backup_files_failed", node_errors => Failures})
    end,
    FileList = [FileInfo || {_Node, {ok, FileInfos}} <- Successes, FileInfo <- FileInfos],
    lists:sort(
        fun(#{created_at_sec := T1, filename := F1}, #{created_at_sec := T2, filename := F2}) ->
            case T1 =:= T2 of
                true -> F1 >= F2;
                false -> T1 > T2
            end
        end,
        FileList
    ).

backup_file_info_example() ->
    #{
        created_at => <<"2023-11-23T19:13:19+02:00">>,
        created_at_sec => 1700759599,
        filename => <<"emqx-export-2023-11-23-19-13-19.043.tar.gz">>,
        node => 'emqx@127.0.0.1',
        size => 22740
    }.

export_request_example() ->
    #{
        table_sets => [
            <<"banned">>,
            <<"builtin_authn">>,
            <<"builtin_authz">>
        ],
        root_keys => [
            <<"connectors">>,
            <<"actions">>,
            <<"sources">>,
            <<"rule_engine">>,
            <<"schema_registry">>
        ]
    }.

files_response_example() ->
    #{
        data => [
            #{
                created_at => <<"2023-09-02T11:11:33+02:00">>,
                created_at_sec => 1693645893,
                filename => <<"emqx-export-2023-09-02-11-11-33.012.tar.gz">>,
                node => 'emqx@127.0.0.1',
                size => 22740
            },
            #{
                created_at => <<"2023-11-23T19:13:19+02:00">>,
                created_at_sec => 1700759599,
                filename => <<"emqx-export-2023-11-23-19-13-19.043.tar.gz">>,
                node => 'emqx@127.0.0.1',
                size => 22740
            }
        ],
        meta => #{
            page => 0,
            limit => 20,
            count => 300
        }
    }.

%% This function is called when running smoke test.
check_desc() ->
    Current = lists:sort(emqx_mgmt_data_backup:all_table_set_names()),
    {ok, Map} = hocon:load(filename:join([code:priv_dir(emqx_dashboard), "desc.en.hocon"])),
    DescPath = "emqx_mgmt_api_data_backup.table_sets.desc",
    Desc = hocon_maps:deep_get(DescPath, Map, map),
    DescLines = binary:split(Desc, <<"\n">>, [global]),
    Matched = lists:filter(
        fun(Line) ->
            re:run(Line, <<"^\\s*-\\s*`[a-zA-Z0-9_]+`:">>, [unicode]) =/= nomatch
        end,
        DescLines
    ),
    %% Extract just the table set names (the part inside backticks)
    Documented = lists:sort(
        lists:map(
            fun(Line) ->
                {match, [Name]} = re:run(Line, <<"`([a-zA-Z0-9_]+)`">>, [
                    {capture, [1], binary}, unicode
                ]),
                Name
            end,
            Matched
        )
    ),
    Current =:= Documented orelse
        error(#{
            reason => "table_sets_desc_needs_update",
            documented => Documented,
            current => Current
        }).
