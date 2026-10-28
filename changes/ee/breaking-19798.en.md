Align CoAP Observe registration, cancellation, and notification transmission with RFC 7641 in EMQX 7.0.

When a client rejects a confirmable or non-confirmable notification with Reset, or a confirmable notification exhausts its retransmissions, EMQX removes the associated Observe relationship. The shared broker subscription is removed only when no observers remain. It also clears the relationship's in-flight notifications, queued notifications, and Block2 state. Other Observe relationships on the same client remain active.

Unlike EMQX 6.x, later resource changes are no longer delivered through the failed relationship. Clients must establish a new Observe relationship to resume receiving notifications.

Multiple Tokens can observe the same resource independently. Explicit deregistration must use the registered Token and now returns `2.05 Content` without an Observe option instead of the previous nonstandard `2.07` response.

Observe sequence numbers are refreshed when notifications are sent, including queued notifications and retransmissions. Non-confirmable notifications share a three-second transmission interval per client. EMQX retains the latest representation for each observation and sends a confirmable notification after 24 hours to confirm continued interest, even if no further resource changes arrive.
