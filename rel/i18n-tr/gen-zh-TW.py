#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate rel/i18n-tr/desc.zh-TW.hocon from the Simplified Chinese translation.

The Simplified Chinese source is emqx/emqx-i18n's desc.zh.hocon, which
scripts/pre-compile.sh installs as apps/emqx_dashboard/priv/desc.zh.hocon.
It is a flattened HOCON file, one `namespace.id.tag = "text"` per line.

Conversion is OpenCC's `s2twp` (Simplified -> Traditional with Taiwan phrases)
followed by the override table below, which fixes the terms s2twp gets wrong or
converts too eagerly for Taiwanese technical writing.

    pip install opencc-python-reimplemented
    ./rel/i18n-tr/gen-zh-TW.py [path/to/desc.zh.hocon]

Re-run this whenever emqx/emqx-i18n publishes a new desc.zh.hocon, then review
the diff before committing.  Anything still missing afterwards shows the English
text, see emqx_dashboard_desc_cache:lookup/5.
"""
import argparse
import collections
import json
import os
import re
import sys

try:
    import opencc
except ImportError:
    sys.exit("opencc is missing: pip install opencc-python-reimplemented")

# The term table is shared with emqx/emqx-dashboard5, where the same file sits
# at scripts/i18n/zh-TW-terms.json, so the Dashboard UI and these descriptions
# use the same wording. Keep the two copies identical.
TERMS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "zh-TW-terms.json")

# Namespace-scoped term overrides, for a word whose right translation depends
# on which module it appears in. Applied before KEY_TERMS.
# 提供者 is the general word; an external commercial service is a 供應商, the way
# an ISP is. The AI providers are OpenAI and Anthropic; an ACL provider and an
# identity provider are not vendors.
NS_TERMS = {
    "emqx_ai_completion_api": [("提供者", "供應商")],
    "emqx_ai_completion_schema": [("提供者", "供應商")],
}

# Per-key term overrides, for the entries where the shared table would be wrong.
# These replace a word inside one entry rather than the whole text, so the rest
# of the entry still tracks the Simplified Chinese source.
# 刪除 is right for the 139 entries whose English says "delete"; these are the
# ones that say "remove".
KEY_TERMS = {
    "emqx_mgmt_api_alarms.delete_alarms_api.desc": [("刪除", "移除")],
    "emqx_mgmt_api_alarms.delete_alarms_api.label": [("刪除", "移除")],
    "emqx_mgmt_api_banned.delete_banned_api.desc": [("刪除", "移除")],
    "emqx_conf_schema.cluster_autoclean.desc": [("刪除", "移除")],
    "emqx_mgmt_api_plugins.sync_plugin_desc.desc": [("刪除", "移除")],
    "emqx_cluster_link_schema.enable.desc": [("刪除", "移除")],
    "emqx_gateway_schema.gateway_mountpoint.desc": [("刪除", "移除")],
    # Elasticsearch documents, not files
    "emqx_bridge_es.action_delete.desc": [("刪除", "移除"), ("檔案", "文件")],
    "emqx_bridge_es.action_create.desc": [("檔案", "文件")],
    "emqx_bridge_es.action_update.desc": [("檔案", "文件")],
    # the English is just "the remote broker"; the Simplified source adds a 服务器
    "emqx_bridge_mqtt_connector_schema.connect_timeout.desc":
        [("broker 伺服器的", "broker 的")],
    # "the broker pool workers"; the Simplified source calls it a 代理服务器
    # 线程池, which the proxy rules then turn into a Proxy 伺服器
    "emqx_schema.sysmon_broker_pool_mailbox_size_alarm_threshold.desc":
        [("Proxy 伺服器執行緒池", "broker 池")],
    # the English is "Timespan for response", not for the request
    "emqx_coap_api.timeout.desc": [("請求逾時", "回應逾時")],
    # the label reads better without the redundant 時間
    "emqx_schema.mqtt_listener_proxy_protocol_timeout.label":
        [("Proxy 協定逾時時間", "Proxy 協定逾時")],
    # the English is "The SO_REUSEADDR flag", not an identifier
    "emqx_schema.fields_tcp_opts_reuseaddr.desc": [("識別", "旗標")],
    # a query filter is 篩選 in Taiwan; 過濾器 is kept for the MQTT topic
    # filter, which is the spec's own term
    "emqx_authn_mongodb_schema.filter.desc": [("過濾", "篩選")],
    "emqx_authz_mongodb_schema.filter.desc": [("過濾", "篩選")],
    "emqx_dashboard_sso_ldap.filter.desc": [("過濾", "篩選")],
    "emqx_ldap.filter.desc": [("過濾", "篩選")],
    "emqx_prometheus_api.qp_namespace.desc": [("過濾", "篩選")],
    # the English is "Proxy address header" / "Proxy port header"; the
    # Simplified source mistranslates Proxy as 客户端. The same fields in
    # emqx_gateway_schema are translated correctly.
    "emqx_schema.fields_ws_opts_proxy_address_header.label":
        [("用戶端地址頭", "Proxy 地址請求標頭")],
    "emqx_schema.fields_ws_opts_proxy_port_header.label":
        [("用戶端連接埠頭", "Proxy 連接埠請求標頭")],
    "emqx_otel_schema.exporter_headers.desc":
        [("頭是一個以頭名稱為鍵", "標頭是一個以標頭名稱為鍵")],
    # the Simplified source lost the line breaks here and merged the next four
    # entries into this value; they have their own keys, so drop the tail
    "emqx_jt808_schema.jt808_allow_anonymous.desc":
        [("。registry_url.desc", "。__CUT__")],
    # bare "Proxy" labels; 代理 is reserved for an agent
    "emqx_bridge_snowflake_aggregated_action_schema.proxy_config.label":
        [("代理", "Proxy")],
    "emqx_bridge_snowflake_aggregated_connector_schema.proxy_config.label":
        [("代理", "Proxy")],
    # a security profile, not a file
    "emqx_mgmt_api_data_backup.allow_security_profile_mismatch.desc": [
        ("`hardened` 檔案", "`hardened` 設定檔"),
    ],
}

# Per-key replacements applied last, for texts that are stale or wrong in the
# Simplified Chinese source no matter how they are converted.  Keyed by the full
# doc id as it appears in the file.
CORRECTIONS = {
    # the English label is "List Provider Models"
    "emqx_ai_completion_api.ai_providers_model_list.label": "列出供應商模型",
    # these authz endpoints come in clientid / username pairs, so the field name
    # is what tells them apart; the Simplified source translates it away
    "emqx_authz_api_mnesia.user_clientid_get.desc":
        "取得指定 clientid 的規則",
    "emqx_authz_api_mnesia.user_clientid_get.label":
        "取得指定 clientid 的規則",
    "emqx_authz_api_mnesia.user_clientid_put.desc":
        "設定指定 clientid 的規則",
    "emqx_authz_api_mnesia.user_clientid_put.label":
        "設定指定 clientid 的規則",
    "emqx_authz_api_mnesia.user_clientid_delete.desc":
        "刪除指定 clientid 的規則",
    "emqx_authz_api_mnesia.user_clientid_delete.label":
        "刪除指定 clientid 的規則",
    "emqx_authz_api_mnesia.users_clientid_post.desc":
        "為指定 clientid 新增規則。",
    "emqx_authz_api_mnesia.users_clientid_post.label":
        "為指定 clientid 新增規則",
    "emqx_authz_api_mnesia.fuzzy_clientid.desc":
        "以子字串模糊搜尋 clientid",
    "emqx_authz_api_mnesia.user_username_get.desc":
        "取得指定 username 的規則",
    "emqx_authz_api_mnesia.user_username_get.label":
        "取得指定 username 的規則",
    "emqx_authz_api_mnesia.user_username_put.desc":
        "設定指定 username 的規則",
    "emqx_authz_api_mnesia.user_username_put.label":
        "設定指定 username 的規則",
    "emqx_authz_api_mnesia.user_username_delete.desc":
        "刪除指定 username 的規則",
    "emqx_authz_api_mnesia.user_username_delete.label":
        "刪除指定 username 的規則",
    "emqx_authz_api_mnesia.users_username_post.desc":
        "為指定 username 新增規則。",
    "emqx_authz_api_mnesia.users_username_post.label":
        "為指定 username 新增規則",
    "emqx_authz_api_mnesia.fuzzy_username.desc":
        "以子字串模糊搜尋 username",
    "emqx_authn_mnesia_schema.user_id_type.desc":
        "指定認證時使用 `clientid` 還是 `username`。",
    # the English is "List all AI models for a provider type."; the Simplified
    # source adds an "available for" the original does not have
    "emqx_ai_completion_api.ai_model_list.desc":
        "列出特定供應商類型的所有 AI 模型",
    # the upstream zh text still names the old "en / zh" language tags
    "emqx_dashboard_schema.i18n_lang.desc":
        "設定 Swagger 文件與設定項說明的語言，可為 en、zh 或 zh-TW，預設為 en。",
}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    default_src = os.path.join(here, "..", "..", "apps", "emqx_dashboard",
                               "priv", "desc.zh.hocon")
    ap = argparse.ArgumentParser()
    ap.add_argument("source", nargs="?", default=default_src,
                    help="flattened Simplified Chinese desc file")
    ap.add_argument("-o", "--output",
                    default=os.path.join(here, "desc.zh-TW.hocon"))
    args = ap.parse_args()

    if not os.path.isfile(args.source):
        sys.exit("source not found: %s\nrun scripts/pre-compile.sh first, or pass "
                 "the path to emqx-i18n's desc.zh.hocon" % args.source)

    with open(TERMS_FILE, encoding="utf-8") as f:
        terms = json.load(f)["terms"]
    overrides = [(t["from"], t["to"], bool(t.get("regex"))) for t in terms]
    labels = [t["to"] for t in terms]

    conv = opencc.OpenCC("s2twp")
    hits = collections.Counter()
    out, seen = [], set()
    n_values = n_skipped = 0

    with open(args.source, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            m = re.match(r'^([^\s=]+)\s*=\s*"(.*)"\s*$', line)
            if not m:
                if line.strip():
                    n_skipped += 1
                out.append(line)
                continue
            key, value = m.group(1), m.group(2)
            seen.add(key)
            text = conv.convert(value)
            for idx, (pattern, replacement, is_regex) in enumerate(overrides):
                if is_regex:
                    text, n = re.subn(pattern, replacement, text)
                else:
                    n = text.count(pattern)
                    if n:
                        text = text.replace(pattern, replacement)
                hits[idx] += n
            for word, replacement in NS_TERMS.get(key.split(".")[0], ()):
                text = text.replace(word, replacement)
            for word, replacement in KEY_TERMS.get(key, ()):
                text = text.replace(word, replacement)
            text = text.split("__CUT__")[0]
            text = CORRECTIONS.get(key, text)
            out.append('%s = "%s"' % (key, text))
            n_values += 1

    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out))
        if out and out[-1] != "":
            f.write("\n")

    stale = [k for k in list(CORRECTIONS) + list(KEY_TERMS) if k not in seen]
    print("source : %s" % os.path.normpath(args.source))
    print("output : %s" % os.path.normpath(args.output))
    print("values : %d converted, %d non key/value lines passed through"
          % (n_values, n_skipped))
    print("per-key overrides: %d applied, %d stale (key no longer in the source)"
          % (len(CORRECTIONS) + len(KEY_TERMS) - len(stale), len(stale)))
    for k in stale:
        print("   STALE: %s" % k)
    print("override hits:")
    for idx, label in enumerate(labels):
        if hits[idx]:
            print("   %-10s %5d" % (label, hits[idx]))


if __name__ == "__main__":
    main()
