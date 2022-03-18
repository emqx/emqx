%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% The destination URL for the telemetry data report
-define(TELEMETRY_URL, "https://telemetry.emqx.io/api/telemetry").

%% Interval for reporting telemetry data, Default: 7d
-define(REPORT_INTERVAL, 604800).

-define(API_TAG_MQTT, [<<"mqtt">>]).
-define(API_SCHEMA_MODULE, emqx_modules_schema).
