/*
 * Copyright (c) 2013 Jan-Piet Mens <jpmens()gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of mosquitto nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include "base64.h"
#include "erl_nif.h"
#include "emqttd_plugin_mysql_app.h"

#define KEY_LENGTH      24
#define SEPARATOR       "$"
#define SEPARATOR1       "_"
#define TRUE	(1)
#define FALSE	(0)

/*
 * Split PBKDF2$... string into their components. The caller must free()
 * the strings.
 */

static int detoken(char *pbkstr, char **sha, int *iter, char **salt, char **key)
{
    char *p, *s, *save;
    int rc = 1;

    save = s = strdup(pbkstr);

    if ((p = strsep(&s, SEPARATOR1)) == NULL)
        goto out;
    if (strcmp(p, "pbkdf2") != 0)
        goto out;

    if ((p = strsep(&s, SEPARATOR)) == NULL)
        goto out;
    *sha = strdup(p);

    if ((p = strsep(&s, SEPARATOR)) == NULL)
        goto out;
    *iter = atoi(p);

    if ((p = strsep(&s, SEPARATOR)) == NULL)
        goto out;
    *salt = strdup(p);

    if ((p = strsep(&s, SEPARATOR)) == NULL)
        goto out;
    *key = strdup(p);

    rc = 0;

out:
    free(save);
    return rc;
}

    ERL_NIF_TERM
pbkdf2_check(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret;
    ErlNifBinary binps, binhash;
    emqttd_plugin_mysql_app_st* st = enif_alloc(sizeof(emqttd_plugin_mysql_app_st));
    if(st == NULL) {
        return make_atom(env, "alloc_error");
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

    if(argc != 2) {
        return make_error(st, env, "Bad args");
    } else if(!enif_inspect_binary(env, argv[0], &binps)|!enif_inspect_binary(env, argv[1], &binhash)) {
        return make_error(st, env, "Bad args password or username inspect error");
    }

    char* password = (char*)binps.data;
    char* hash = (char*)binhash.data;
    static char *sha, *salt, *h_pw;
    int iterations, saltlen, blen;
    char *b64, *keybuf;
    unsigned char *out;
    int match = FALSE;
    const EVP_MD *evpmd;
    int keylen, rc;

    if (detoken(hash, &sha, &iterations, &salt, &h_pw) != 0)
        return match;

    /* Determine key length by decoding base64 */
    if ((keybuf = malloc(strlen(h_pw) + 1)) == NULL) {
        return make_error(st, env, "internal_error: Out Of memory");
    }
    keylen = base64_decode(h_pw, keybuf);
    if (keylen < 1) {
        free(keybuf);
        return make_atom(env, "false");
    }
    free(keybuf);

    if ((out = malloc(keylen)) == NULL) {
        return make_error(st, env, "Cannot allocate out; out of memory\n");
    }

#ifdef PWDEBUG
    fprintf(stderr, "sha        =[%s]\n", sha);
    fprintf(stderr, "iterations =%d\n", iterations);
    fprintf(stderr, "salt       =[%s]\n", salt);
    fprintf(stderr, "h_pw       =[%s]\n", h_pw);
    fprintf(stderr, "kenlen     =[%d]\n", keylen);
#endif

    saltlen = strlen((char *)salt);

    evpmd = EVP_sha256();
    if (strcmp(sha, "sha1") == 0) {
        evpmd = EVP_sha1();
    } else if (strcmp(sha, "sha512") == 0) {
        evpmd = EVP_sha512();
    }

    rc = PKCS5_PBKDF2_HMAC(password, strlen(password),
            (unsigned char *)salt, saltlen,
            iterations,
            evpmd, keylen, out);
    if (rc != 1) {
        goto out;
    }

    blen = base64_encode(out, keylen, &b64);
    if (blen > 0) {
        int i, diff = 0, hlen = strlen(h_pw);
#ifdef PWDEBUG
        fprintf(stderr, "HMAC b64   =[%s]\n", b64);
#endif

        /* "manual" strcmp() to ensure constant time */
        for (i = 0; (i < blen) && (i < hlen); i++) {
            diff |= h_pw[i] ^ b64[i];
        }

        match = diff == 0;
        if (hlen != blen)
            match = 0;

        free(b64);
    }

out:
    free(sha);
    free(salt);
    free(h_pw);
    free(out);

    if(match == 0){
        ret = make_atom(env, "false");
    }else{
        ret = make_atom(env, "true");
    }
    return ret;
}

int pbkdf2_check_native(char *password, char *hash)
{
    static char *sha, *salt, *h_pw;
    int iterations, saltlen, blen;
    char *b64;
    unsigned char key[128];
    int match = FALSE;
    const EVP_MD *evpmd;

    if (detoken(hash, &sha, &iterations, &salt, &h_pw) != 0)
        return match;

#ifdef PWDEBUG
    fprintf(stderr, "sha        =[%s]\n", sha);
    fprintf(stderr, "iterations =%d\n", iterations);
    fprintf(stderr, "salt       =[%s]\n", salt);
    fprintf(stderr, "h_pw       =[%s]\n", h_pw);
#endif

    saltlen = strlen((char *)salt);

    evpmd = EVP_sha256();
    if (strcmp(sha, "sha1") == 0) {
        evpmd = EVP_sha1();
    } else if (strcmp(sha, "sha512") == 0) {
        evpmd = EVP_sha512();
    }

    PKCS5_PBKDF2_HMAC(password, strlen(password),
            (unsigned char *)salt, saltlen,
            iterations,
            evpmd, KEY_LENGTH, key);

    blen = base64_encode(key, KEY_LENGTH, &b64);
    if (blen > 0) {
        int i, diff = 0, hlen = strlen(h_pw);
#ifdef PWDEBUG
        fprintf(stderr, "HMAC b64   =[%s]\n", b64);
#endif

        /* "manual" strcmp() to ensure constant time */
        for (i = 0; (i < blen) && (i < hlen); i++) {
            diff |= h_pw[i] ^ b64[i];
        }

        match = diff == 0;
        if (hlen != blen)
            match = 0;

        free(b64);
    }

    free(sha);
    free(salt);
    free(h_pw);

    return match;
}
int main()
{
    // char password[] = "hello";
    // char PB1[] = "PBKDF2$sha256$10000$eytf9sEo8EprP9P3$2eO6tROHiqI3bm+gg+vpmWooWMpz1zji";
    char password[] = "supersecret";
    //char PB1[] = "PBKDF2$sha256$10000$YEbSTt8FaMRDq/ib$Kt97+sMCYg00mqMOBAYinqZlnxX8HqHk";
    char PB1[] = "pbkdf2_sha256$10000$YEbSTt8FaMRDq/ib$Kt97+sMCYg00mqMOBAYinqZlnxX8HqHk";
    // char PB1[] = "PBKDF2$sha1$10000$XWfyPLeC9gsD6SbI$HOnjU4Ux7RpeBHdqYxpIGH1R5qCCtNA1";
    // char PB1[] = "PBKDF2$sha512$10000$v/aaCgBZ+VZN5L8n$BpgjSTyb4weVxr9cA2mvQ+jaCyaAPeYe";
    int match;

    printf("Checking password [%s] for %s\n", password, PB1);

    match = pbkdf2_check_native(password, PB1);
    printf("match == %d\n", match);
    return match;
}
