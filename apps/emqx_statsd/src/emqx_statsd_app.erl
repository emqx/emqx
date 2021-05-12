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

-module(emqx_statsd_app).

-behaviour(application).

-include("emqx_statsd.hrl").

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    Host = application:get_env(?APP, host, ?DEFAULT_HOST),
    Port = application:get_env(?APP, port, ?DEFAULT_PORT),
    Prefix = application:get_env(?APP, prefix, ?DEFAULT_PREFIX),
    Tags = application:get_env(?APP, tags, ?DEFAULT_TAGS),
    BatchSize = application:get_env(?APP, batch_size, ?DEFAULT_BATCH_SIZE),
    estatsd:start_link([{host, Host},
                        {port, Port},
                        {prefix, Prefix},
                        {tags, Tags},
                        {batch_size, BatchSize}]),
    SampleTimeInterval = application:get_env(?APP, sample_time_interval, ?DEFAULT_SAMPLE_TIME_INTERVAL),
    FlushTimeInterval = application:get_env(?APP, flush_time_interval, ?DEFAULT_FLUSH_TIME_INTERVAL),
    emqx_statsd_sup:start_link([{sample_time_interval, SampleTimeInterval},
                                {flush_time_interval, FlushTimeInterval}]).

stop(_State) ->
    ok.

