# Real unreachable branches (CT only) - 2026-02-08

Strategy (list only, do not change code)
- Only list branches that are proven unreachable by real config/data flow
- Provide file + line + reason
- No code removal in this round

Current list

LwM2M
- apps/emqx_gateway_lwm2m/src/emqx_lwm2m_cmd.erl:476
  Reason: safe_hexstr_to_bin/1 catches `throw` from emqx_utils:hexstr_to_bin/1, but for binaries the size is
  always a multiple of 8 bits, so `Size rem 16` is only 0 or 8 and the throw branch cannot happen.
- apps/emqx_gateway_lwm2m/src/emqx_lwm2m_session.erl:640
  Reason: depends on emqx_coap_blockwise:send_next_client_tx_block/3 returning consume_only, but that branch
  is unreachable because set_payload_block always sets a valid block1 option and get_option returns it.
- apps/emqx_gateway_lwm2m/src/emqx_lwm2m_session.erl:810
  Reason: downlink_ctx_key/1 non-map clause is not reachable from exported paths; all real Ctx values are maps.

CoAP
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:268
  Reason: handle_server_tx_block2/4 is only called if should_split_server_tx_block2/3 succeeds; that function
  already calls pick_server_tx_block2_params/2, so the error branch here cannot be reached.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:350
  Reason: apply_server_tx_template/4 always sets block2 via set_payload_block/4, so get_option(block2, ...) never
  returns a non-tuple value here.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:381
  Reason: reply_request/2 second clause is only used when Req is not a coap_message, but the only call sites
  that can reach it are error branches that require a valid Req.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:515
  Reason: send_next_client_tx_block/3 always sets a valid block1 option; get_option(block1, ...) returns
  {Num, More, Size} so the consume_only branch cannot happen.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:560
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:603
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:635
  Reason: block2_template/3 always returns a request or response record; it never returns undefined in the
  current data flow, so next_block2_request/5 never returns undefined.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:780
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:790
  Reason: emqx_schema:to_bytesize/1 and to_duration_ms/1 return {ok, Int} or {error, _}, but the code
  matches on an integer directly, so the integer branch is unreachable.
- apps/emqx_gateway_coap/src/emqx_coap_blockwise.erl:799
  Reason: is_valid_block_size/1 is only called with integers (guards in call sites). The non-integer clause
  is unreachable.
- apps/emqx_gateway_coap/src/emqx_coap_session.erl:286-287
  Reason: maybe_split_notify_block2/4 calls server_prepare_out_response/4 with Req = undefined; this path
  can return single or chunked, but not {error, _} in real flow.
- apps/emqx_gateway_coap/src/emqx_coap_tm.erl:230
  Reason: emqx_coap_transport never returns timeouts => [], it either omits the key or returns a non-empty list,
  so process_timeouts([], ...) cannot be reached.
- apps/emqx_gateway_coap/src/emqx_coap_api.erl:183
  Reason: depends on emqx_coap_blockwise consume_only branch, which is unreachable.
- apps/emqx_gateway_coap/src/emqx_coap_api.erl:206
  Reason: block2_exceeds_max_body/2 is only called after client_in_response returns send_next, which implies a
  valid block2 option with More = true; the fallback clause cannot match.
- apps/emqx_gateway_coap/src/emqx_coap_api.erl:217
  Reason: block2_too_large_reply/2 always receives Ctx with request in maybe_collect_block2/4; the fallback
  clause is unreachable.
