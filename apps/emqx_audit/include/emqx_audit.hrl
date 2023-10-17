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

-define(AUDIT, emqx_audit).

-record(?AUDIT, {
    seq,
    %% basic info
    created_at,
    node,
    from,
    source,
    source_ip,
    %% operation info
    operation_id,
    operation_type,
    args,
    operation_result,
    failure,
    %% request detail
    http_method,
    http_request,
    http_status_code,
    duration_ms,
    extra
}).
