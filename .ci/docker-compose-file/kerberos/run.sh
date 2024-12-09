#!/bin/sh


echo "Remove old keytabs"

rm -f /var/lib/secret/kafka.keytab > /dev/null 2>&1
rm -f /var/lib/secret/rig.keytab > /dev/null 2>&1

rm -f /var/lib/secret/erlang.keytab > /dev/null 2>&1
rm -f /var/lib/secret/krb_authn_cli.keytab > /dev/null 2>&1

echo "Create realm"

kdb5_util -P emqx -r KDC.EMQX.NET create -s

echo "Add principals"

kadmin.local -w password -q "add_principal -randkey kafka/kafka-1.emqx.net@KDC.EMQX.NET" > /dev/null
kadmin.local -w password -q "add_principal -randkey kafka/kafka-2.emqx.net@KDC.EMQX.NET" > /dev/null
kadmin.local -w password -q "add_principal -randkey rig@KDC.EMQX.NET"  > /dev/null

# For Kerberos Authn
kadmin.local -w password -q "add_principal -randkey mqtt/erlang.emqx.net@KDC.EMQX.NET" > /dev/null
kadmin.local -w password -q "add_principal -randkey krb_authn_cli@KDC.EMQX.NET"  > /dev/null


echo "Create keytabs"

kadmin.local -w password -q "ktadd  -k /var/lib/secret/kafka.keytab -norandkey kafka/kafka-1.emqx.net@KDC.EMQX.NET " > /dev/null
kadmin.local -w password -q "ktadd  -k /var/lib/secret/kafka.keytab -norandkey kafka/kafka-2.emqx.net@KDC.EMQX.NET " > /dev/null
kadmin.local -w password -q "ktadd  -k /var/lib/secret/rig.keytab -norandkey rig@KDC.EMQX.NET " > /dev/null

# For Kerberos Authn
kadmin.local -w password -q "ktadd  -k /var/lib/secret/erlang.keytab -norandkey mqtt/erlang.emqx.net@KDC.EMQX.NET " > /dev/null
kadmin.local -w password -q "ktadd  -k /var/lib/secret/krb_authn_cli.keytab -norandkey krb_authn_cli@KDC.EMQX.NET " > /dev/null

echo STARTING KDC
/usr/sbin/krb5kdc -n
