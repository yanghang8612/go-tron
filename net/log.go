package net

import gtronlog "github.com/tronprotocol/go-tron/common/log"

// log is the package-level structured logger. Each line carries module=net;
// the message text disambiguates which subsystem (sync / handler / pbft) the
// record came from.
var log = gtronlog.NewModule("net")
