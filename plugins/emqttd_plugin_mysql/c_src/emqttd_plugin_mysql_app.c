// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.

#include "emqttd_plugin_mysql_app.h"

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    emqttd_plugin_mysql_app_st* st = enif_alloc(sizeof(emqttd_plugin_mysql_app_st));
    if(st == NULL) {
        return 1;
    }

    st->atom_ok = make_atom(env, "ok");
    st->atom_error = make_atom(env, "error");
    st->atom_null = make_atom(env, "null");
    st->atom_true = make_atom(env, "true");
    st->atom_false = make_atom(env, "false");
    st->atom_bignum = make_atom(env, "bignum");
    st->atom_bignum_e = make_atom(env, "bignum_e");
    st->atom_bigdbl = make_atom(env, "bigdbl");
    st->atom_partial = make_atom(env, "partial");
    st->atom_uescape = make_atom(env, "uescape");
    st->atom_pretty = make_atom(env, "pretty");
    st->atom_force_utf8 = make_atom(env, "force_utf8");

    // Markers used in encoding
    st->ref_object = make_atom(env, "$object_ref$");
    st->ref_array = make_atom(env, "$array_ref$");

    *priv = (void*) st;

    return 0;
}

static int
reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    return load(env, priv, info);
}

static void
unload(ErlNifEnv* env, void* priv)
{
    enif_free(priv);
    return;
}

static ErlNifFunc funcs[] =
{
    {"nif_pbkdf2_check", 2, pbkdf2_check}
};

ERL_NIF_INIT(emqttd_plugin_mysql_app, funcs, &load, &reload, &upgrade, &unload);
