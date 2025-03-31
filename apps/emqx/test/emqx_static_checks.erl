%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_static_checks).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    logger:notice(
        asciiart:visible(
            $=,
            "If this test suite failed, and you are unsure why, read this:~n"
            "https://github.com/emqx/emqx/blob/master/apps/emqx/src/bpapi/README.md",
            []
        )
    ).

check_if_versions_consistent(OldData, NewData) ->
    %% OldData can contain a wider list of BPAPI versions
    %% than the release being checked.
    [] =:= NewData -- OldData.

t_run_check(_) ->
    try
        {ok, OldData} = file:consult(emqx_bpapi_static_checks:versions_file()),
        ?assert(emqx_bpapi_static_checks:run()),
        {ok, NewData} = file:consult(emqx_bpapi_static_checks:versions_file()),
        check_if_versions_consistent(OldData, NewData) orelse
            begin
                logger:critical(
                    asciiart:visible(
                        $=,
                        "BPAPI versions were changed, but not committed to the repo.\n\n"
                        "Versions file is generated automatically, to update it, run\n"
                        "'make && make static_checks' locally, and then add the\n"
                        "changed 'bpapi.versions' files to the commit.\n",
                        []
                    )
                ),
                error(version_mismatch)
            end,
        BpapiDumps = filelib:wildcard(
            filename:join(
                emqx_bpapi_static_checks:dumps_dir(),
                "*" ++ emqx_bpapi_static_checks:dump_file_extension()
            )
        ),
        logger:info("Backplane API dump files: ~p", [BpapiDumps]),
        ?assert(emqx_bpapi_static_checks:check_compat(BpapiDumps))
    catch
        error:version_mismatch ->
            error(tc_failed);
        EC:Err:Stack ->
            logger:critical("Test suite failed: ~p:~p~nStack:~p", [EC, Err, Stack]),
            error(tc_failed)
    end.
