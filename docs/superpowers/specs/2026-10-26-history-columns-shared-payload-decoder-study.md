# Bounded HSP1 scratch study before physical format registration

The complete 37-member HSP1 page evidence is pinned at SHA256
1f1148ee9885c9efcc793ac3058826b5029fe77f7b9941b83c5f456864bd03c5.
All 1,817 rows are locally verified. The prototype saves 555,250 page bytes on
the three frozen outputs and reduces resident encode time by 39.09%, while
decode time increases 10.12%. It remains unregistered and opt-in product policy
has not changed. Do not generalize page timings to publication or HTTP.

Preserve the successful native lab, inputs, source and evidence unchanged.
Next compare its exact HSP1 frame bytes with the local scratch decoder. Use the
same pinned original V5 files and require exact old stored controls, identical
HSP1 output SHA per page and identical canonical recovery in both decoders.
Keep every page, including all 11 ordinary fallbacks and any slower result.

The replacement may reuse one <=128 KiB + 36 B transformed buffer per caller.
Use full slice expressions to enforce the exact key and common-frame output
caps even when the backing array is larger. Retained scratch must never alias
returned canonical bytes; concurrent callers have separate scratch. Keep all
existing header, integer, key mode, CRC and malformed-frame checks. The local
boundary/recovery/eight-caller race checks are evidence only for this local
implementation, not native real-page performance.

Use a separate overlay-only bounded unit after the previous study has exited.
Use explicit native Go 1.25.5 paths, GOPATH/GOMODCACHE and existing cache; no
production environment change. Record matched fresh-buffer and reused-buffer
comparisons separately, alternate order, retain per-page raw timings and source,
binary and input identity. No registered container, HTTP or MuHash claim at
this stage. Any decision to integrate still requires actual whole files,
publication, complete HTTP, commitment, historical query and recovery gates.
No deployment, service operation, reset, deletion or budget change.
