%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(OMA_ALTER_PATH_RT, <<"\"oma.lwm2m\"">>).

-define(REG_PREFIX, <<"rd">>).

%%--------------------------------------------------------------------
%% Data formats for transferring resource information, defined in
%% OMA-TS-LightweightM2M-V1_0_1-20170704-A

%% 0: Plain text. 0 is numeric value used in CoAP Content-Format option.
%% The plain text format is used for "Read" and "Write" operations on singular
%% Resources. i.e: /3/0/0
%%
%% This data format has a Media Type of "text/plain".
% Unused
% -define(LWM2M_FORMAT_PLAIN_TEXT, 0).

%% 40: Link format. 40 is numeric value used in CoAP Content-Format option.
%%
-define(LWM2M_FORMAT_LINK, 40).

%% 42: Opaque. 41 is numeric value used in CoAP Content-Format option.
%% The opaque format is used for "Read" and "Write" operations on singular
%% Resources where the value of the Resource is an opaque binary value.
%% i.e: firmware images or opaque value from top layer.
%%
%% This data format has a Media Type of "application/octet-stream".
% Unused
% -define(LWM2M_FORMAT_OPAQUE, 42).

%% 11542: TLV. 11542 is numeric value used in CoAP Content-Format option.
%% For "Read" and "Write" operation, the binary TLV format is used to represent
%% an array of values or a single value using a compact binary representation.
%%
%% This data format has a Media Type of "application/vnd.oma.lwm2m+tlv".
% Unused
% -define(LWM2M_FORMAT_TLV, 11542).

%% 11543: JSON. 11543 is numeric value used in CoAP Content-Format option.
%% The client may support the JSON format for "Read" and "Write" operations to
%% represent multiple resource or single resource values.
%%
%% This data format has a Media Type of "application/vnd.oma.lwm2m+json".
% Unused
% -define(LWM2M_FORMAT_OMA_JSON, 11543).
