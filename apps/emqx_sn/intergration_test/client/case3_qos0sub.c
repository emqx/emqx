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
 * Normal topic name is automatically registered at subscription, then
 * a message is published and the node receives it itself
 *******************************************************************************/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "MQTTSNPacket.h"
#include "transport.h"

#include "int_test_result.h"



#define TLOG(fmt, ...)  tlog("qos0sub", fmt,  ## __VA_ARGS__)
#define PRE_DEF_TOPIC_ID 1


char * read_publish(char *host, int port, char * buf, int buflen, MQTTSN_topicid *recv_pubtopic)
{
    int rc = 0;
    int len = 0;
    
    if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_PUBLISH)
    {
        unsigned short packet_id;
        int qos, payloadlen;
        unsigned char* payload = NULL;
        unsigned char dup, retained;
        MQTTSN_topicid pubtopic;

        if (MQTTSNDeserialize_publish(&dup, &qos, &retained, &packet_id, &pubtopic,
                &payload, &payloadlen, buf, buflen) != 1)
            TLOG("Error deserializing publish\n");
        else 
            TLOG("publish received, packet_id %d qos %d topictype %d topicid %d\n", packet_id, qos, pubtopic.type, pubtopic.data.id);
        
        recv_pubtopic->type = pubtopic.type;
        recv_pubtopic->data.id = pubtopic.data.id;

        if (qos == 1)
        {
            len = MQTTSNSerialize_puback(buf, buflen, pubtopic.data.id, packet_id, MQTTSN_RC_ACCEPTED);
            rc = transport_sendPacketBuffer(host, port, buf, len);
            if (rc == 0)
                TLOG("puback sent\n");
        }
        
        return payload;
    }
    
    return NULL;
}


int main(int argc, char** argv)
{
    int rc = 0;
    int mysock;
    unsigned char buf[200];
    int buflen = sizeof(buf);
    MQTTSN_topicid topic;
    char  expect_payload[16];
    int len = 0;
    int i = 0;
    unsigned char dup = 0;
    int qos = 1;
    unsigned char retained = 0;
    short packetid = 1;
    //char *topicname = "predef_topic1";
    unsigned short predef_topicid1 = PRE_DEF_TOPIC_ID;
    unsigned short predef_topicid2 = PRE_DEF_TOPIC_ID+1;
    char *host = "127.0.0.1";
    int port = 1884;
    MQTTSNPacket_connectData options = MQTTSNPacket_connectData_initializer;
    unsigned short topicid;
    char * recv_payload = NULL;
    char   final_result = RESULT_PASS;

    mysock = transport_open();
    if(mysock < 0)
        return mysock;

    if (argc > 1)
        host = argv[1];

    if (argc > 2)
        port = atoi(argv[2]);

    TLOG("Sending to hostname %s port %d\n", host, port);

    options.clientID.cstring = "subpredef0 MQTT-SN";
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


    /* subscribe */
    TLOG("Subscribing to predef_topicid1\n");
    topic.type = MQTTSN_TOPIC_TYPE_PREDEFINED;
    topic.data.id = predef_topicid1;
    len = MQTTSNSerialize_subscribe(buf, buflen, 0, 0, packetid, &topic);
    rc = transport_sendPacketBuffer(host, port, buf, len);

    if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_SUBACK)     /* wait for suback */
    {
        unsigned short submsgid;
        int granted_qos;
        unsigned char returncode;

        rc = MQTTSNDeserialize_suback(&granted_qos, &predef_topicid1, &submsgid, &returncode, buf, buflen);
        if (granted_qos != 0 || returncode != 0)
        {
            TLOG("granted qos != 2, %d return code %d\n", granted_qos, returncode);
            goto exit;
        }
        else
            TLOG("suback topic id %d\n", predef_topicid1);
    }
    else
        goto exit;

    packetid = 2;
    TLOG("Subscribing to predef_topicid2\n");
    topic.type = MQTTSN_TOPIC_TYPE_PREDEFINED;
    topic.data.id = predef_topicid2;
    len = MQTTSNSerialize_subscribe(buf, buflen, 0, 0, packetid, &topic);
    rc = transport_sendPacketBuffer(host, port, buf, len);

    if (MQTTSNPacket_read(buf, buflen, transport_getdata) == MQTTSN_SUBACK)     /* wait for suback */
    {
        unsigned short submsgid;
        int granted_qos;
        unsigned char returncode;

        rc = MQTTSNDeserialize_suback(&granted_qos, &predef_topicid2, &submsgid, &returncode, buf, buflen);
        if (granted_qos != 0 || returncode != 0)
        {
            TLOG("granted qos != 2, %d return code %d\n", granted_qos, returncode);
            goto exit;
        }
        else
            TLOG("suback topic id %d\n", predef_topicid2);
    }
    else
        goto exit;

    /*receive publish*/
    TLOG("Receive publish\n");
    for(i=0;i<2;i++)
    {
        MQTTSN_topicid pubtopic = {0};
        char ascii = 'a'+(i%26);
        expect_payload[0] = expect_payload[1] = expect_payload[2] = ascii;
        expect_payload[3] = 0;
        recv_payload = read_publish(host, port, buf, buflen, &pubtopic);
        if( recv_payload )
        {
            TLOG("case1_qos0sub %d receive %s\n", i, recv_payload);
            if( strcmp(recv_payload, expect_payload) != 0 )
            {
                final_result = RESULT_FAIL;
                break;
            }
        }
        if((pubtopic.type != MQTTSN_TOPIC_TYPE_PREDEFINED) || (pubtopic.data.id != predef_topicid1+i))
        {
            final_result = RESULT_FAIL;
            break;
        }
        sleep(1);
    }
    

    len = MQTTSNSerialize_disconnect(buf, buflen, 0);
    rc = transport_sendPacketBuffer(host, port, buf, len);

exit:
    transport_close();

    mark_result(argv[0], final_result);
    
    return 0;
}




