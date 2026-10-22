# Inactive one-attempt Linux B versus B+A2 packet

Read PLAN.md before use. Source baseline is tested B commit572dcc377584be867542d0a56e0faf164cb1aa41.
Darwin A2 remains NO-GO. This directory alone changes no production source.
All copied Go helpers have .txt suffixes; no Go package/embedding input is added.

Root/Astra review and explicit server mutation GO are required before these
example commands. The audited outer release shell must already pause/drain auto
deploy and own inherited FD9 for /data/gtron/start.lock; no packet command stops
or starts a service or obtains the helper's root release lock.

1. Fetch the reviewed docs-only carrier via GitHub and verify packet-manifest.json.
2. `/usr/bin/python3 prepare.py --repo /data/gtron/go-tron --output /data/gtron/go-tron/build/linux-a2.NAME --start-lock-fd 9`
3. Review correctness, complete source/binary freeze and resource identities.
4. `/usr/bin/python3 run.py --phase counts --output /data/gtron/go-tron/build/linux-a2.NAME --start-lock-fd 9`
5. Audit the sole counts proof and freeze; obtain formal-window GO.
6. `/usr/bin/python3 run.py --phase timing --output /data/gtron/go-tron/build/linux-a2.NAME --start-lock-fd 9`
7. `/usr/bin/python3 analyze.py /data/gtron/go-tron/build/linux-a2.NAME`
8. `/usr/bin/python3 archive-evidence.py /data/gtron/go-tron/build/linux-a2.NAME`

Never rerun a failed or already attempted stage. Recover all raw evidence through
own JumpServer SFTP, verify archive/per-file SHA, and obtain independent review.
Any failed correctness/environment/performance gate selects Bv2 only. Linux all
PASS merely admits A2 to final combined review; full exact-commit Linux Sapling
whole-repo/build and explicit Root release GO still precede the one final deploy.
