
import socket

content = "\x03\x01\x10"

print("client start to send\n")
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
print(["send searchgw\n", content])
s.sendto(content, ("192.168.222.255", 1884))
data, addr = s.recvfrom(1024)
gwinfo = "\x03\x02\x01"
if data != gwinfo:
    print("ERROR, expect GWINFO %s\n"%gwinfo)
    print("But receive %s\n", data)
else:
    print("PASS\n")

s.close()

