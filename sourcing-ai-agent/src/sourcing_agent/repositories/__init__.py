"""Track B B4.2 — per-domain control-plane repositories.

Each module here owns one domain's ``TableDescriptor``s + a ``Repository`` over the PG adapter
primitives, decomposing the inherited 28k-line ``ControlPlaneStore`` God-class. Callers migrate to these
repositories directly (owner-ratified 2026-06-21); ``ControlPlaneStore`` delegates during the transition.
"""
