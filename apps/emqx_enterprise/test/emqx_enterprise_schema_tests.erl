%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

injected_roots_test() ->
    ExpectedRoots = lists:usort(ee_schema_roots()),
    InjectedRoots = lists:usort(get_roots(emqx_enterprise_schema)),
    MissingRoots = ExpectedRoots -- InjectedRoots,
    ?assertEqual([], MissingRoots, #{
        missing => ExpectedRoots -- InjectedRoots,
        unknown => InjectedRoots -- ExpectedRoots,
        hint =>
            <<
                "maybe there's a missing schema module to be added to"
                " `EE_SCHEMA_MODULES' or `EXTRA_SCHEMA_MODULES' in"
                " `emqx_enterprise_schema'"
            >>
    }),
    ok.

ee_schema_roots() ->
    #{ee_business_apps := Apps} = emqx_machine_boot:read_apps(),
    lists:append([ee_schema_roots(atom_to_list(App)) || App <- Apps]).

ee_schema_roots(AppName) ->
    lists:foldl(
        fun(Filepath, Acc) ->
            Mod = module(Filepath),
            case has_roots(Mod) of
                true ->
                    get_roots(Mod) ++ Acc;
                false ->
                    Acc
            end
        end,
        [],
        filelib:wildcard("apps/" ++ AppName ++ "/src/**/*_schema.erl")
    ).

module(Filepath) ->
    ModStr = filename:basename(Filepath, ".erl"),
    list_to_atom(ModStr).

get_roots(Mod) ->
    lists:map(
        fun
            ({Root, _Sc}) ->
                Root;
            (Root) when is_atom(Root) ->
                Root
        end,
        Mod:roots()
    ).

has_roots(Mod) ->
    try
        _ = Mod:module_info(),
        erlang:function_exported(Mod, roots, 0) andalso
            Mod:roots() =/= []
    catch
        error:undef ->
            %% may have picked a module from an app that's not in the enterprise release.
            ct:pal("module not found?  ~s", [Mod]),
            false
    end.
