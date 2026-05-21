package net

import gtronlog "github.com/tronprotocol/go-tron/common/log"

var (
	log     = gtronlog.NewModule("net/handler")
	syncLog = gtronlog.NewModule("net/sync")
	pbftLog = gtronlog.NewModule("net/pbft")
)
