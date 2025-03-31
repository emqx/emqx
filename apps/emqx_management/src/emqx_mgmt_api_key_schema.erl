%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_key_schema).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

namespace() -> api_key.
roots() -> ["api_key"].

fields("api_key") ->
    [
        {bootstrap_file,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bootstrap_file),
                    required => false,
                    default => <<>>
                }
            )}
    ].

desc("api_key") ->
    ?DESC(api_key).
