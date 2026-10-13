%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Internal representation shared by the hook dispatcher and manager.
-record(callback, {
    action :: emqx_hooks:action(),
    filter :: undefined | emqx_hooks:filter(),
    priority :: integer()
}).
