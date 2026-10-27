For in-memory sessions, renamed the internal message header `deliver_begin_at` to `deliver_begin_at_us` and changed its timestamp unit from milliseconds to microseconds.

Plugins and hook handlers that read this header must use the new name and adapt to the new precision.
