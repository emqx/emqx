
input = open("emqx-rel/deps/emqx_sn/etc/emqx_sn.conf")
lines = input.readlines()
input.close()

output  = open("emqx-rel/deps/emqx_sn/etc/emqx_sn.conf",'w');
for line in lines:
    if not line:
        break
    if 'enable_qos3' in line:
        line = line.replace('off','on')
        output.write(line)
    else:
        output.write(line)
output.close()


