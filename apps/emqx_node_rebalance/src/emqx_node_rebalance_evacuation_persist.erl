%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_node_rebalance_evacuation_persist).

-export([save/1,
         clear/0,
         read/0]).

-include("emqx_node_rebalance.hrl").
-include_lib("emqx/include/types.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type start_opts() :: #{redirect_to => emqx_eviction_agent:server_reference(),
                        conn_evict_rate => integer()}.

-spec save(start_opts()) -> ok_or_error(term()).
save(#{conn_evict_rate := ConnEvictRate, server_reference := ServerReference} = Data)
  when is_integer(ConnEvictRate) andalso
       (is_binary(ServerReference) orelse ServerReference =:= undefined) ->
    Filepath = evacuation_filepath(),
    case filelib:ensure_dir(Filepath) of
        ok ->
            JsonData = emqx_json:encode(Data, [pretty]),
            file:write_file(Filepath, JsonData);
        {error, _} = Error -> Error
    end.

-spec clear() -> ok.
clear() ->
    file:delete(evacuation_filepath()).

-spec read() -> {ok, start_opts()} | none.
read() ->
    case file:read_file(evacuation_filepath()) of
        {ok, Data} ->
            case emqx_json:safe_decode(Data, [return_maps]) of
                {ok, Map} when is_map(Map) ->
                    {ok, #{
                           conn_evict_rate =>
                                maps:get(<<"conn_evict_rate">>, Map, ?DEFAULT_CONN_EVICT_RATE),
                           server_reference =>
                                maps:get(<<"server_reference">>, Map, undefined)
                          }};
                _NotAMap ->
                    {ok, #{
                           conn_evict_rate => ?DEFAULT_CONN_EVICT_RATE,
                           server_reference => undefined
                          }}
            end;
        {error, _} ->
            none
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

evacuation_filepath() ->
    filename:join([emqx:get_env(data_dir), ?EVACUATION_FILENAME]).
