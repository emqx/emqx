// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.

#ifndef EMQTTD_PLUGIN_MYSQL_APP_H
#define EMQTTD_PLUGIN_MYSQL_APP_H

#include "erl_nif.h"

typedef struct {
    ERL_NIF_TERM    atom_ok;
    ERL_NIF_TERM    atom_error;
    ERL_NIF_TERM    atom_null;
    ERL_NIF_TERM    atom_true;
    ERL_NIF_TERM    atom_false;
    ERL_NIF_TERM    atom_bignum;
    ERL_NIF_TERM    atom_bignum_e;
    ERL_NIF_TERM    atom_bigdbl;
    ERL_NIF_TERM    atom_partial;
    ERL_NIF_TERM    atom_uescape;
    ERL_NIF_TERM    atom_pretty;
    ERL_NIF_TERM    atom_force_utf8;

    ERL_NIF_TERM    ref_object;
    ERL_NIF_TERM    ref_array;
} emqttd_plugin_mysql_app_st;

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name);
ERL_NIF_TERM make_ok(emqttd_plugin_mysql_app_st* st, ErlNifEnv* env, ERL_NIF_TERM data);
ERL_NIF_TERM make_error(emqttd_plugin_mysql_app_st* st, ErlNifEnv* env, const char* error);

ERL_NIF_TERM pbkdf2_check(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

int int_from_hex(const unsigned char* p);
int int_to_hex(int val, char* p);
int utf8_len(int c);
int utf8_esc_len(int c);
int utf8_validate(unsigned char* data, size_t size);
int utf8_to_unicode(unsigned char* buf, size_t size);
int unicode_to_utf8(int c, unsigned char* buf);
int unicode_from_pair(int hi, int lo);
int unicode_uescape(int c, char* buf);
int double_to_shortest(char *buf, size_t size, size_t* len, double val);

#endif // Included EMQTTD_PLUGIN_MYSQL_APP_H
