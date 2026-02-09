# LwM2M + CoAP coverage (CT only) - 2026-02-08

Scope
- Apps: emqx_gateway_lwm2m, emqx_gateway_coap
- Tests: Common Test only (no EUnit include, no meck in LwM2M/CoAP suites)
- Allow direct function calls inside CT where needed

Commands (Docker)
- docker exec -t erlang  bash -c "export ENABLE_COVER_COMPILE=1; make apps/emqx_gateway_lwm2m-ct"
- docker exec -t erlang  bash -c "export ENABLE_COVER_COMPILE=1; make apps/emqx_gateway_coap-ct"
- cover:analyse_to_file/3 for all modules in both apps (HTML in _build/.../cover/aggregate)

Results (2026-02-08)
- LwM2M CT: 123 ok, 0 failed
- CoAP CT: 154 ok, 0 failed
- cover:analyse(Module, line) zero-hit lines:
  - emqx_lwm2m_cmd: [476]
  - emqx_lwm2m_session: [640, 810]
  - emqx_coap_blockwise: [268, 350, 381, 515, 560, 603, 635, 780, 790, 799]
  - emqx_coap_session: [286, 287]
  - emqx_coap_tm: [230]
  - emqx_coap_api: [183, 206, 217]
  - emqx_lwm2m_channel / emqx_coap_channel / emqx_coap_transport: []
  - See notes/coverage/lwm2m-coap-unreachable-branches.md for reasons

Notes
- Real HTTP authn/authz used via emqx_gateway_auth_ct; hook error paths use real hook registration
- Open-session error paths use real ekka_locker contention (separate process holds lock)
- CoAP gateway bad config path uses real UDP port bind conflict
- Added CT case for invalid block2 option type (no EUnit/mock)
