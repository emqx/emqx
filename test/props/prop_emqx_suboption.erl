%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(prop_emqx_suboption).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% -type subopts() :: #{ rh => 0 | 1
%%                     , rap => 0 | 1
%%                     , nl => 0 | 1
%%                     , qos => 0 | 1
%%                     }.
%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------
prop_symmetric() ->
    ?FORALL(SubOpt,
            subopts(),
            begin
                SubOpt == compress_then_decompress(SubOpt)
            end).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
compress_then_decompress(Input) ->
    emqx_broker:decompress(emqx_broker:compress(Input)).

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------
subopts() ->
    oneof([mandatory_subopts(), mandatory_and_optional_subopts()]).

mandatory_subopts() ->
    ?LET(Opts,
         [ { rh, range(0, 1)}
         , { rap, range(0, 1)}
         , { nl, range(0, 1)}
         , { qos, range(0, 2)}
         ],
         maps:from_list(Opts)).

mandatory_and_optional_subopts() ->
    ?LET({Mandatory, Optional},
         {mandatory_subopts(),
          [ {shard, range(0, 65535)}
          , {atom(), binary()}
          ]
         },
         maps:merge(Mandatory, maps:from_list(Optional))).
