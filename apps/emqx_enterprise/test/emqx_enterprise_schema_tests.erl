%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
