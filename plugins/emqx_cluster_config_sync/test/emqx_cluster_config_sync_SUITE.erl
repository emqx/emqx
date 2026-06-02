%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        t_enabled_secondary,
        t_interval_ms,
        t_sync_once_exports_downloads_imports_and_cleans_up,
        t_sync_once_reports_import_errors,
        t_sync_once_reports_remote_export_errors
    ].

t_enabled_secondary(_Config) ->
    ?assertEqual(false, emqx_cluster_config_sync:enabled_secondary(#{})),
    ?assertEqual(
        false,
        emqx_cluster_config_sync:enabled_secondary(#{
            <<"enable">> => true,
            <<"role">> => <<"primary">>
        })
    ),
    ?assertEqual(
        true,
        emqx_cluster_config_sync:enabled_secondary(#{
            <<"enable">> => true,
            <<"role">> => <<"secondary">>
        })
    ).

t_interval_ms(_Config) ->
    ?assertEqual(60000, emqx_cluster_config_sync:interval_ms(sync_config(<<"1m">>))),
    ?assertEqual(300000, emqx_cluster_config_sync:interval_ms(sync_config(<<"invalid">>))).

t_sync_once_exports_downloads_imports_and_cleans_up(_Config) ->
    Self = self(),
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    BackupBin = <<"backup-bytes">>,
    Deps = #{
        request_fun => fun(Method, Url, Headers, Body, Timeout) ->
            Self ! {request, Method, Url, Headers, Body, Timeout},
            case Method of
                post -> {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
                get -> {ok, 200, [], BackupBin};
                delete -> {ok, 204, [], <<>>}
            end
        end,
        upload_fun => fun(Filename, Bin) ->
            Self ! {upload, Filename, Bin},
            ok
        end,
        import_fun => fun(Filename) ->
            Self ! {import, Filename},
            {ok, #{db_errors => #{}, config_errors => #{}}}
        end,
        delete_local_fun => fun(Filename) ->
            Self ! {delete_local, Filename},
            ok
        end
    },

    ?assertMatch(
        {ok, #{filename := BackupName}}, emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ),

    receive
        {request, post, ExportUrl, Headers, ExportBody, 30000} ->
            ?assertEqual(true, lists:suffix("/data/export", ExportUrl)),
            ?assertEqual(
                {"Authorization", "Basic " ++ base64:encode_to_string(<<"key:secret">>)},
                lists:keyfind("Authorization", 1, Headers)
            ),
            ExportReq = emqx_utils_json:decode(ExportBody),
            ?assertEqual(default_root_keys(), maps:get(<<"root_keys">>, ExportReq)),
            ?assertEqual([], maps:get(<<"table_sets">>, ExportReq))
    after 0 ->
        error(missing_export_request)
    end,
    assert_request(get, "/data/files/" ++ binary_to_list(BackupName), BackupBin),
    assert_seen({upload, BackupName, BackupBin}),
    assert_seen({import, BackupName}),
    assert_request(delete, "/data/files/" ++ binary_to_list(BackupName), <<>>),
    assert_seen({delete_local, BackupName}).

t_sync_once_reports_import_errors(_Config) ->
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    Deps = #{
        request_fun => fun
            (post, _Url, _Headers, _Body, _Timeout) ->
                {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
            (get, _Url, _Headers, undefined, _Timeout) ->
                {ok, 200, [], <<"backup">>};
            (delete, _Url, _Headers, undefined, _Timeout) ->
                {ok, 204, [], <<>>}
        end,
        upload_fun => fun(_Filename, _Bin) -> ok end,
        import_fun => fun(_Filename) ->
            {ok, #{db_errors => #{}, config_errors => #{[<<"rules">>] => {error, bad_rule}}}}
        end,
        delete_local_fun => fun(_Filename) -> ok end
    },
    ?assertMatch(
        {error, {import_failed, {ok, #{config_errors := #{}}}}},
        emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ).

t_sync_once_reports_remote_export_errors(_Config) ->
    Deps = #{
        request_fun => fun(post, _Url, _Headers, _Body, _Timeout) ->
            {ok, 500, [], <<"boom">>}
        end,
        upload_fun => fun(_Filename, _Bin) -> error(unexpected_upload) end,
        import_fun => fun(_Filename) -> error(unexpected_import) end,
        delete_local_fun => fun(_Filename) -> error(unexpected_delete_local) end
    },
    ?assertMatch(
        {error, {http_error, post, _, 500, <<"boom">>}},
        emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ).

sync_config(Interval) ->
    #{<<"sync">> => #{<<"interval">> => Interval}}.

conf() ->
    #{
        <<"enable">> => true,
        <<"role">> => <<"secondary">>,
        <<"primary">> => #{
            <<"base_url">> => <<"http://primary:18083/api/v5/">>,
            <<"api_key">> => <<"key">>,
            <<"api_secret">> => <<"secret">>
        },
        <<"sync">> => #{
            <<"interval">> => <<"5m">>,
            <<"timeout">> => <<"30s">>,
            <<"root_keys">> => default_root_keys(),
            <<"table_sets">> => [],
            <<"delete_remote_backup">> => true,
            <<"delete_local_backup">> => true
        }
    }.

default_root_keys() ->
    emqx_cluster_config_sync_client:default_root_keys().

assert_request(Method, UrlSuffix, Body) ->
    receive
        {request, Method, Url, _Headers, _ReqBody, _Timeout} ->
            ?assertEqual(true, lists:suffix(UrlSuffix, Url)),
            ok;
        {request, Method, Url, _Headers, undefined, _Timeout} when Method =:= get ->
            ?assertEqual(true, lists:suffix(UrlSuffix, Url)),
            ok
    after 0 ->
        error({missing_request, Method, UrlSuffix, Body})
    end.

assert_seen(Msg) ->
    receive
        Msg -> ok
    after 0 ->
        error({missing_message, Msg})
    end.
