%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Logging mechanism
%%------------------------------------------------------------------------------
-define(PRINT(Format, Args),
    io:format(Format, Args)).

-define(PRINT_MSG(Msg),
    io:format(Msg)).

-define(DEBUG(Format, Args),
    lager:debug(Format, Args)).

-define(DEBUG_TRACE(Dest, Format, Args),
    lager:debug(Dest, Format, Args)).

-define(DEBUG_MSG(Msg),
    lager:debug(Msg)).

-define(INFO(Format, Args),
    lager:info(Format, Args)).

-define(INFO_TRACE(Dest, Format, Args),
    lager:info(Dest, Format, Args)).

-define(INFO_MSG(Msg),
    lager:info(Msg)).

-define(WARN(Format, Args),
    lager:warning(Format, Args)).

-define(WARN_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARN_MSG(Msg),
    lager:warning(Msg)).
			      
-define(WARNING(Format, Args),
    lager:warning(Format, Args)).

-define(WARNING_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARNING_MSG(Msg),
    lager:warning(Msg)).

-define(ERROR(Format, Args),
    lager:error(Format, Args)).

-define(ERROR_TRACE(Dest, Format, Args),
    lager:error(Dest, Format, Args)).

-define(ERROR_MSG(Msg),
    lager:error(Msg)).

-define(CRITICAL(Format, Args),
    lager:critical(Format, Args)).

-define(CRITICAL_TRACE(Dest, Format, Args),
    lager:critical(Dest, Format, Args)).

-define(CRITICAL_MSG(Msg),
    lager:critical(Msg)).

