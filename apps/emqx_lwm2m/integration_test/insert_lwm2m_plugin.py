

def change_makefile():
    f = open("emqx-rel/Makefile", "rb")
    data = f.read()
    f.close()

    if data.find("emqx_lwm2m") < 0:
        data = data.replace("emqx_auth_pgsql emqx_auth_mongo", "emqx_auth_pgsql emqx_auth_mongo emqx_lwm2m\n\ndep_emqx_lwm2m = git https://github.com/emqx/emqx-lwm2m\n\n")
        f = open("emqx-rel/Makefile", "wb")
        f.write(data)
        f.close()
        

    f = open("emqx-rel/relx.config", "rb")
    data = f.read()
    f.close()

    if data.find("emq_lwm2m") < 0:
        f = open("emqx-rel/relx.config", "wb")
        data = data.replace("{emqx_auth_mongo, load}", "{emqx_auth_mongo, load},\n{emqx_lwm2m, load}")
        data = data.replace('{template, "rel/conf/emqx.conf", "etc/emqx.conf"},', \
                '{template, "rel/conf/emqx.conf", "etc/emqx.conf"},'+  \
                '\n    {template, "rel/conf/plugins/emqx_lwm2m.conf", "etc/plugins/emqx_lwm2m.conf"},'+  \
                '\n    {copy, "deps/emqx_lwm2m/lwm2m_xml", "etc/"},')
        f.write(data)
        f.close()
        
        
        
def change_lwm2m_config():
    f = open("emqx-rel/deps/emqx_lwm2m/etc/emqx_lwm2m.conf", "rb")
    data = f.read()
    f.close()
    
    if data.find("5683") > 0:
        data = data.replace("5683", "5683")
        f = open("emqx-rel/deps/emqx_lwm2m/etc/emqx_lwm2m.conf", "wb")
        f.write(data)
        f.close()
    
    

def main():
    change_makefile()
    change_lwm2m_config()
    
    
if __name__ == "__main__":
    main()
    
    