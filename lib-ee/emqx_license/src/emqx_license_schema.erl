%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_schema).

-include_lib("typerefl/include/types.hrl").

%%------------------------------------------------------------------------------
%% hocon_schema callbacks
%%------------------------------------------------------------------------------

-behaviour(hocon_schema).

-export([roots/0, fields/1]).

roots() -> [{license, hoconsc:union(
                        [hoconsc:ref(?MODULE, key_license),
                         hoconsc:ref(?MODULE, file_license)])}].

fields(key_license) ->
    [ {key, #{type => string(),
              sensitive => true, %% so it's not logged
              desc => "Configure the license as a string"
             }}
    ];
fields(file_license) ->
    [ {file, #{type => string(),
               desc => "Path to the license file"
              }}
    ].
