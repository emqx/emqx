%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(RESOURCE_NOT_FOUND, 'RESOURCE_NOT_FOUND').
-define(INTERNAL_ERROR, 'INTERNAL_SERVER_ERROR').

-define(STANDARD_RESP(R), (begin
    R
end)#{
    400 => emqx_dashboard_swagger:error_codes(
        [?BAD_REQUEST], <<"Bad request">>
    ),
    404 => emqx_dashboard_swagger:error_codes(
        [?NOT_FOUND, ?RESOURCE_NOT_FOUND], <<"Not Found">>
    )
}).
