%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(dq_config(Path), emqx_ds_shared_sub_config:get(Path)).
-define(dq_config(Path, Default), emqx_ds_shared_sub_config:get(Path, Default)).
