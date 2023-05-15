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


#define TLOG(fmt, ...)  tlog("case7: ", fmt,  ## __VA_ARGS__)


int connect(MQTTSNPacket_connectData *options, char *host, int port, unsigned char *buf, int buflen)
{
    int len = 0, rc = 0;
    int connack_rc = -1;
    TLOG("Sending to hostname %s port %d\n", host, port);
    len = MQTTSNSerialize_connect(buf, buflen, options);
    if((rc = transport_sendPacketBuffer(host, port, buf, len)) != 0)
    {
        TLOG("Send connect failed, rc:%d\n", rc);
        return rc;
    }

    /* wait for connack */
    if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_CONNACK)
    {
        if (MQTTSNDeserialize_connack(&connack_rc, buf, buflen) != 1 || connack_rc != 0)
        {
            TLOG("Unable to connect, return code %d\n", connack_rc);
        }
        else
            TLOG("connected rc %d\n", connack_rc);
    }

    return connack_rc;
}


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


    options.clientID.cstring = "testclientid_case7";
    options.duration = 100;
    if((rc = connect(&options, host, port, buf, buflen) != 0))
        goto exit;

    sleep(2);

    // a new client-id on the same udp port will be refused by emq-sn
    options.clientID.cstring = "testclientid_case7_new";
    TLOG("will send connect with new clientId:%s\n", options.clientID.cstring);
    if((rc = connect(&options, host, port, buf, buflen)) == 0)
        goto exit;

    sleep(2);

    // go back to the old client-id and the connection will be successful
    options.clientID.cstring = "testclientid_case7";
    TLOG("will send connect with old clientId:%s\n", options.clientID.cstring);
    if((rc = connect(&options, host, port, buf, buflen)) != 0)
        goto exit;

    sleep(2);

    options.duration = 90;
    TLOG("will send connect with new duration:%d\n", options.duration);
    if((rc = connect(&options, host, port, buf, buflen)) == 0)
        goto exit;

    sleep(2);

    options.duration = 100;
    TLOG("will send connect with old duration:%d\n", options.duration);
    if((rc = connect(&options, host, port, buf, buflen)) != 0)
        goto exit;

	mark_result(argv[0], RESULT_PASS);
	transport_close();
	return 0;

exit:
	mark_result(argv[0], RESULT_FAIL);
    transport_close();

    return 0;
}




