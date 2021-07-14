/*******************************************************************************
 * Copyright (c) 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    Sergio R. Caprile - clarifications and/or documentation extension
 *
 * Description:
 * Short topic name used to avoid registration process
 *******************************************************************************/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>

#include "MQTTSNPacket.h"
#include "transport.h"

#include "int_test_result.h"


#define TLOG(fmt, ...)  tlog("case6: ", fmt,  ## __VA_ARGS__)





int main(int argc, char** argv)
{
    int rc = 0;
    int mysock;
    unsigned char buf[200];
    int buflen = sizeof(buf);
    
    unsigned char payload[16];
    int payloadlen = 0;
    int len = 0;
    int qos = 0;
    int retained = 0;
    short packetid = 0;
    char ascii = 0;
//  char *topicname = "a long topic name";
    char *host = "127.0.0.1";
    int port = 1884;
    int i = 0;
    MQTTSNString msstr = {0};
    MQTTSNPacket_connectData options = MQTTSNPacket_connectData_initializer;

    mysock = transport_open();
    if(mysock < 0)
        return mysock;

    if (argc > 1)
        host = argv[1];

    if (argc > 2)
        port = atoi(argv[2]);

    TLOG("Sending to hostname %s port %d\n", host, port);

    options.clientID.cstring = "testclientid_case1";
    len = MQTTSNSerialize_connect(buf, buflen, &options);
    rc = transport_sendPacketBuffer(host, port, buf, len);

    /* wait for connack */
    if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_CONNACK)
    {
        int connack_rc = -1;

        if (MQTTSNDeserialize_connack(&connack_rc, buf, buflen) != 1 || connack_rc != 0)
        {
            TLOG("Unable to connect, return code %d\n", connack_rc);
            goto exit;
        }
        else 
            TLOG("connected rc %d\n", connack_rc);
    }
    else
        goto exit;

    
    
    len = MQTTSNSerialize_disconnect(buf, buflen, 5);
    rc = transport_sendPacketBuffer(host, port, buf, len);
    /* wait for DISCONNECT */
    if ((rc = MQTTSNPacket_read(buf, buflen, transport_getdata)) == MQTTSN_DISCONNECT)
    {
        int duration = 0;
        if (MQTTSNDeserialize_disconnect(&duration, buf, buflen) != 1)
        {
            TLOG("Unable to diconnect, return code %d\n", duration);
            goto exit;
        }
        else 
            TLOG("duration %d\n", duration);
    }
    else
    {
    	TLOG("rc %d\n", rc);
        goto exit;
    }


    sleep(2);


    msstr.cstring = "testclientid_case1";
    len = MQTTSNSerialize_pingreq(buf, buflen, msstr);
    rc = transport_sendPacketBuffer(host, port, buf, len);
    /* wait for PINGRSP */
	if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_PINGRESP)
	{
		if (MQTTSNDeserialize_pingresp(buf, buflen) != 1)
		{
			TLOG("Unable to ping\n");
			goto exit;
		}
		else
			TLOG("get pingresp\n");
	}
	else
		goto exit;


	mark_result(argv[0], RESULT_PASS);
	transport_close();
	return 0;

exit:
	mark_result(argv[0], RESULT_FAIL);
    transport_close();

    return 0;
}




