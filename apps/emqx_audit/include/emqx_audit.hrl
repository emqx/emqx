%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(AUDIT, emqx_audit).

-record(?AUDIT, {
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
