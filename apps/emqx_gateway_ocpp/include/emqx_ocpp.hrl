%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% %%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-define(APP, emqx_ocpp).

%% types for ocppj-1.6
-define(OCPP_MSG_TYPE_ID_CALL, 2).
-define(OCPP_MSG_TYPE_ID_CALLRESULT, 3).
-define(OCPP_MSG_TYPE_ID_CALLERROR, 4).
%% actions for ocppj-1.6
-define(OCPP_ACT_Authorize, <<"Authorize">>).
-define(OCPP_ACT_BootNotification, <<"BootNotification">>).
-define(OCPP_ACT_CancelReservation, <<"CancelReservation">>).
-define(OCPP_ACT_ChangeAvailability, <<"ChangeAvailability">>).
-define(OCPP_ACT_ChangeConfiguration, <<"ChangeConfiguration">>).
-define(OCPP_ACT_ClearCache, <<"ClearCache">>).
-define(OCPP_ACT_ClearChargingProfile, <<"ClearChargingProfile">>).
-define(OCPP_ACT_DataTransfer, <<"DataTransfer">>).
-define(OCPP_ACT_DiagnosticsStatusNotification, <<"DiagnosticsStatusNotification">>).
-define(OCPP_ACT_FirmwareStatusNotification, <<"FirmwareStatusNotification">>).
-define(OCPP_ACT_GetCompositeSchedule, <<"GetCompositeSchedule">>).
-define(OCPP_ACT_GetConfiguration, <<"GetConfiguration">>).
-define(OCPP_ACT_GetDiagnostics, <<"GetDiagnostics">>).
-define(OCPP_ACT_GetLocalListVersion, <<"GetLocalListVersion">>).
-define(OCPP_ACT_Heartbeat, <<"Heartbeat">>).
-define(OCPP_ACT_MeterValues, <<"MeterValues">>).
-define(OCPP_ACT_RemoteStartTransaction, <<"RemoteStartTransaction">>).
-define(OCPP_ACT_RemoteStopTransaction, <<"RemoteStopTransaction">>).
-define(OCPP_ACT_ReserveNow, <<"ReserveNow">>).
-define(OCPP_ACT_Reset, <<"Reset">>).
-define(OCPP_ACT_SendLocalList, <<"SendLocalList">>).
-define(OCPP_ACT_SetChargingProfile, <<"SetChargingProfile">>).
-define(OCPP_ACT_StartTransaction, <<"StartTransaction">>).
-define(OCPP_ACT_StatusNotification, <<"StatusNotification">>).
-define(OCPP_ACT_StopTransaction, <<"StopTransaction">>).
-define(OCPP_ACT_TriggerMessage, <<"TriggerMessage">>).
-define(OCPP_ACT_UnlockConnector, <<"UnlockConnector">>).
-define(OCPP_ACT_UpdateFirmware, <<"UpdateFirmware">>).
%% error codes for ocppj-1.6
-define(OCPP_ERR_NotSupported, <<"NotSupported">>).
-define(OCPP_ERR_InternalError, <<"InternalError">>).
-define(OCPP_ERR_ProtocolError, <<"ProtocolError">>).
-define(OCPP_ERR_SecurityError, <<"SecurityError">>).
-define(OCPP_ERR_FormationViolation, <<"FormationViolation">>).
-define(OCPP_ERR_PropertyConstraintViolation, <<"PropertyConstraintViolation">>).
-define(OCPP_ERR_OccurenceConstraintViolation, <<"OccurenceConstraintViolation">>).
-define(OCPP_ERR_TypeConstraintViolation, <<"TypeConstraintViolation">>).
-define(OCPP_ERR_GenericError, <<"GenericError">>).

-type utf8_string() :: unicode:unicode_binary().

-type message_type() :: ?OCPP_MSG_TYPE_ID_CALL..?OCPP_MSG_TYPE_ID_CALLERROR.

%% OCPP_ACT_Authorize..OCPP_ACT_UpdateFirmware
-type action() :: utf8_string().

-type frame() :: #{
    type := message_type(),
    %% The message ID serves to identify a request.
    %% Maximum of 36 characters, to allow for GUIDs
    id := utf8_string(),
    %% the name of the remote procedure or action.
    %% This will be a case-sensitive string.
    %% Only presented in ?OCPP_MSG_TYPE_ID_CALL
    action => action(),
    %% json map decoded by jsx and validated by json schema
    payload := null | map()
}.

-define(IS_REQ(F), F = #{type := ?OCPP_MSG_TYPE_ID_CALL}).
-define(IS_REQ(F, Id), F = #{type := ?OCPP_MSG_TYPE_ID_CALL, id := Id}).
-define(IS_RESP(F), F = #{type := ?OCPP_MSG_TYPE_ID_CALLRESULT}).
-define(IS_RESP(F, Id), F = #{type := ?OCPP_MSG_TYPE_ID_CALLRESULT, id := Id}).
-define(IS_ERROR(F), F = #{type := ?OCPP_MSG_TYPE_ID_CALLERROR}).
-define(IS_ERROR(F, Id), F = #{type := ?OCPP_MSG_TYPE_ID_CALLERROR, id := Id}).

-define(IS_BootNotification_RESP(Status, Interval), #{
    type := ?OCPP_MSG_TYPE_ID_CALLRESULT,
    payload := #{<<"status">> := Status, <<"interval">> := Interval}
}).

-define(ERR_FRAME(Id, Code, Desc), #{
    id => Id,
    type => ?OCPP_MSG_TYPE_ID_CALLERROR,
    error_code => Code,
    error_desc => Desc,
    error_details => null
}).
