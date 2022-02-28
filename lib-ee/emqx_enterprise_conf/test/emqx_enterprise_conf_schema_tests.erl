%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").

doc_gen_test() ->
    Dir = "tmp",
    ok = filelib:ensure_dir(filename:join("tmp", foo)),
    _ = emqx_conf:dump_schema(Dir, emqx_enterprise_conf_schema),
    ok.
