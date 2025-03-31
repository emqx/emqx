%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(RESOURCE_ERROR(Reason, Msg),
    {error, {resource_error, #{reason => Reason, msg => Msg}}}
).
-define(RESOURCE_ERROR_M(Reason, Msg), {error, {resource_error, #{reason := Reason, msg := Msg}}}).
