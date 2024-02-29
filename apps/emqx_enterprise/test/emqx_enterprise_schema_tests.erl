%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_schema_tests).

-include_lib("eunit/include/eunit.hrl").

doc_gen_test() ->
    %% the json file too large to encode.
    {
        timeout,
        60,
        fun() ->
            Dir = "tmp",
            ok = emqx_conf:dump_schema(Dir, emqx_enterprise_schema)
        end
    }.

audit_log_test() ->
    ensure_acl_conf(),
    Conf0 = <<"node {cookie = aaa, data_dir = \"/tmp\"}, log.audit.enable=true">>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    ConfList = hocon_tconf:generate(emqx_enterprise_schema, ConfMap0),
    Kernel = proplists:get_value(kernel, ConfList),
    Loggers = proplists:get_value(logger, Kernel),
    FileHandlers = lists:filter(fun(L) -> element(3, L) =:= logger_disk_log_h end, Loggers),
    AuditHandler = lists:keyfind(emqx_audit, 2, FileHandlers),
    %% default log level is info.
    ?assertMatch(
        {handler, emqx_audit, logger_disk_log_h, #{
            config := #{
                type := wrap,
                file := "log/audit.log",
                max_no_bytes := _,
                max_no_files := _
            },
            filesync_repeat_interval := no_repeat,
            filters := [{filter_audit, {_, stop}}],
            formatter := _,
            level := info
        }},
        AuditHandler
    ),
    ok.

ensure_acl_conf() ->
    File = emqx_schema:naive_env_interpolation(<<"${EMQX_ETC_DIR}/acl.conf">>),
    ok = filelib:ensure_dir(filename:dirname(File)),
    case filelib:is_regular(File) of
        true -> ok;
        false -> file:write_file(File, <<"">>)
    end.
