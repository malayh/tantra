# Permissions

Permission values are `allow`, `ask`, and `deny`. `strictest()` orders them deny, ask, allow.

The longest matching rule on the current actor's `Agent` wins; equal-length matches use the stricter verdict. A match replaces and may widen or narrow the tool declaration. With no match, the tool declaration applies, then `Runtime.default_permission`. `check_permission()` validates values early.
