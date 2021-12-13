%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([slow_subs/2, encode_record/1, settings/2]).

-import(hoconsc, [mk/2, ref/1]).
-import(emqx_mgmt_util, [bad_request/0]).

-define(FORMAT_FUN, {?MODULE, encode_record}).
-define(APP, emqx_slow_subs).
-define(APP_NAME, <<"emqx_slow_subs">>).

namespace() -> "slow_subscribers_statistics".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/slow_subscriptions", "/slow_subscriptions/settings"].

schema(("/slow_subscriptions")) ->
    #{
      'operationId' => slow_subs,
      delete => #{tags => [<<"slow subs">>],
                  description => <<"Clear current data and re count slow topic">>,
                  parameters => [],
                  'requestBody' => [],
                  responses => #{204 => <<"No Content">>}
                 },
      get => #{tags => [<<"slow subs">>],
               description => <<"Get slow topics statistics record data">>,
               parameters => [ {page, mk(integer(), #{in => query})}
                             , {limit, mk(integer(), #{in => query})}
                             ],
               'requestBody' => [],
               responses => #{200 => [{data, mk(hoconsc:array(ref(record)), #{})}]}
              }
     };

schema("/slow_subscriptions/settings") ->
    #{'operationId' => settings,
      get => #{tags => [<<"slow subs">>],
               description => <<"Get slow subs settings">>,
               responses => #{200 => conf_schema()}
              },
      put => #{tags => [<<"slow subs">>],
               description => <<"Update slow subs settings">>,
               'requestBody' => conf_schema(),
               responses => #{200 => conf_schema()}
              }
     }.

fields(record) ->
    [
     {clientid, mk(string(), #{desc => <<"the clientid">>})},
     {latency, mk(integer(), #{desc => <<"average time for message delivery or time for message expire">>})},
     {type, mk(string(), #{desc => <<"type of the latency, could be average or expire">>})},
     {last_update_time, mk(integer(), #{desc => <<"the timestamp of last update">>})}
    ].

conf_schema() ->
    Ref = hoconsc:ref(emqx_slow_subs_schema, "emqx_slow_subs"),
    hoconsc:mk(Ref, #{}).

slow_subs(delete, _) ->
    ok = emqx_slow_subs:clear_history(),
    {204};

slow_subs(get, #{query_string := QS}) ->
    Data = emqx_mgmt_api:paginate({?TOPK_TAB, [{traverse, last_prev}]}, QS, ?FORMAT_FUN),
    {200, Data}.

encode_record(#top_k{index = ?INDEX(Latency, ClientId),
                     type = Type,
                     last_update_time = Ts}) ->
    #{clientid => ClientId,
      latency => Latency,
      type => Type,
      last_update_time => Ts}.

settings(get, _) ->
    {200, emqx:get_raw_config([?APP_NAME], #{})};

settings(put, #{body := Body}) ->
    {ok, #{config := #{enable := Enable}}} = emqx:update_config([?APP], Body),
    _ = emqx_slow_subs:update_settings(Enable),
    {200, emqx:get_raw_config([?APP_NAME], #{})}.
