
# insert emqx-sn into Makefile
f = open("emqx-rel/Makefile", "r")
data = f.read()
f.close()
if data.find("emqx-sn") < 0:
    f = open("emqx-rel/Makefile", "w")
    data = data.replace("emqx_auth_pgsql emqx_auth_mongo", "emqx_auth_pgsql emqx_auth_mongo emqx_sn")
    data = data.replace("dep_emqx_auth_mongo", "dep_emqx_sn = git https://github.com/emqx/emqx-sn.git develop\ndep_emqx_auth_mongo")
    f.write(data)
    f.close()


f = open("emqx-rel/relx.config", "r")
data = f.read()
f.close()
if data.find("emqx_sn") < 0:
    f = open("emqx-rel/relx.config", "w")
    data = data.replace("{emqx_dashboard, load},", "{emqx_dashboard, load},\n{emqx_sn, load},")
    f.write(data)
    f.close()

