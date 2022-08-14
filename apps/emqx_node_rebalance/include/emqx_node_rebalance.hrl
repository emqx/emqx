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

-define(DEFAULT_CONN_EVICT_RATE, 500).
-define(DEFAULT_SESS_EVICT_RATE, 500).

-define(DEFAULT_WAIT_HEALTH_CHECK, 60). %% sec
-define(DEFAULT_WAIT_TAKEOVER, 60). %% sec

-define(DEFAULT_ABS_CONN_THRESHOLD, 1000).
-define(DEFAULT_ABS_SESS_THRESHOLD, 1000).

-define(DEFAULT_REL_CONN_THRESHOLD, 1.1).
-define(DEFAULT_REL_SESS_THRESHOLD, 1.1).

-define(EVICT_INTERVAL, 1000).

-define(EVACUATION_FILENAME, <<".evacuation">>).
