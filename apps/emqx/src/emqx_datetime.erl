%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_datetime).

-include_lib("typerefl/include/types.hrl").

%% API
-export([ to_epoch_millisecond/1
        , to_epoch_second/1
        ]).
-export([ epoch_to_rfc3339/1
        , epoch_to_rfc3339/2
        ]).

-reflect_type([ epoch_millisecond/0
              , epoch_second/0
             ]).

-type epoch_second() :: integer().
-type epoch_millisecond() :: integer().
-typerefl_from_string({epoch_second/0, ?MODULE, to_epoch_second}).
-typerefl_from_string({epoch_millisecond/0, ?MODULE, to_epoch_millisecond}).

to_epoch_second(DateTime) ->
    to_epoch(DateTime, second).

to_epoch_millisecond(DateTime) ->
    to_epoch(DateTime, millisecond).

to_epoch(DateTime, Unit) ->
    try
        case string:to_integer(DateTime) of
            {Epoch, []} when Epoch >= 0 -> {ok, Epoch};
            {_Epoch, []} -> {error, bad_epoch};
            _ -> {ok, calendar:rfc3339_to_system_time(DateTime, [{unit, Unit}])}
        end
    catch error: _ ->
        {error, bad_rfc3339_timestamp}
    end.

epoch_to_rfc3339(TimeStamp) ->
    epoch_to_rfc3339(TimeStamp, millisecond).

epoch_to_rfc3339(TimeStamp, Unit) when is_integer(TimeStamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(TimeStamp, [{unit, Unit}])).
