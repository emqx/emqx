%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_psk_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_psk.hrl").

-export([
    namespace/0,
    roots/0,
    fields/1
]).

namespace() -> "psk".

roots() -> ["psk_authentication"].

fields("psk_authentication") ->
    #{
        fields => fields(),
        desc => ?DESC(psk_authentication)
    }.

fields() ->
    [
        {enable,
            ?HOCON(boolean(), #{
                %% importance => ?IMPORTANCE_NO_DOC,
                default => false,
                require => true,
                desc => ?DESC(enable)
            })},
        {init_file,
            ?HOCON(binary(), #{
                required => false,
                desc => ?DESC(init_file)
            })},
        {separator,
            ?HOCON(binary(), #{
                default => ?DEFAULT_DELIMITER,
                desc => ?DESC(separator)
            })},
        {chunk_size,
            ?HOCON(integer(), #{
                default => 50,
                desc => ?DESC(chunk_size)
            })}
    ].
