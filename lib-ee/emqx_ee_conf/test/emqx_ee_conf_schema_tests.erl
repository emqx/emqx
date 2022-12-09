%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").

doc_gen_test() ->
    %% the json file too large to encode.
    {
        timeout,
        60,
        fun() ->
            Dir = "tmp",
            ok = filelib:ensure_dir(filename:join("tmp", foo)),
            I18nFile = filename:join([
                "_build",
                "test",
                "lib",
                "emqx_dashboard",
                "priv",
                "i18n.conf"
            ]),
            _ = emqx_conf:dump_schema(Dir, emqx_ee_conf_schema, I18nFile),
            ok
        end
    }.
