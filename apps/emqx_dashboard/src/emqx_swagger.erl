%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_swagger).

-include_lib("typerefl/include/types.hrl").
-import(hoconsc, [mk/2]).

-define(MAX_ROW_LIMIT, 100).

%% API
-export([fields/1]).
-export([error_codes/1, error_codes/2]).

fields(page) ->
    [{page,
        mk(integer(),
            #{
                in => query,
                desc => <<"Page number of the results to fetch.">>,
                default => 1,
                example => 1})
    }];
fields(limit) ->
    [{limit,
        mk(range(1, ?MAX_ROW_LIMIT),
            #{
                in => query,
                desc => iolist_to_binary([<<"Results per page(max ">>,
                    integer_to_binary(?MAX_ROW_LIMIT), <<")">>]),
                default => ?MAX_ROW_LIMIT,
                example => 50
            })
    }].

error_codes(Codes) ->
    error_codes(Codes, <<"Error code to troubleshoot problems.">>).

error_codes(Codes = [_ | _], MsgExample) ->
    [code(Codes), message(MsgExample)].

message(Example) ->
    {message, mk(string(), #{
        desc => <<"Detailed description of the error.">>,
        example => Example
    })}.

code(Codes) ->
    {code, mk(hoconsc:enum(Codes), #{})}.
