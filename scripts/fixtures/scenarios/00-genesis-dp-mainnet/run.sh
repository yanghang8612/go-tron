#!/usr/bin/env bash
# No chain activity needed. jt_wait_ready in the scenario driver already
# confirmed that /wallet/getnowblock returns 200, which means the node has
# loaded the genesis block. Nothing more to do.
exit 0
