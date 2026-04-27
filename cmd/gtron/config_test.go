package main

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

var testWitnessAddr = tcommon.Address{0x01}

func TestMakeDevGenesisFullFeatures(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, true, 21600000)
	dp := g.DynamicProperties

	checks := []string{
		"allow_new_resource_model",
		"allow_creation_of_contracts",
		"allow_tvm_istanbul",
	}
	for _, key := range checks {
		if dp[key] != 1 {
			t.Errorf("expected DynamicProperties[%q] == 1, got %d", key, dp[key])
		}
	}
	if dp["maintenance_time_interval"] != 21600000 {
		t.Errorf("expected maintenance_time_interval == 21600000, got %d", dp["maintenance_time_interval"])
	}
}

func TestMakeDevGenesisNoFullFeatures(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, false, 21600000)
	dp := g.DynamicProperties

	if _, ok := dp["allow_new_resource_model"]; ok {
		if dp["allow_new_resource_model"] != 0 {
			t.Errorf("expected allow_new_resource_model absent or 0, got %d", dp["allow_new_resource_model"])
		}
	}
	if dp["maintenance_time_interval"] != 21600000 {
		t.Errorf("expected maintenance_time_interval == 21600000, got %d", dp["maintenance_time_interval"])
	}
}

func TestMakeDevGenesisCustomInterval(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, false, 30000)
	if g.DynamicProperties["maintenance_time_interval"] != 30000 {
		t.Errorf("expected maintenance_time_interval == 30000, got %d", g.DynamicProperties["maintenance_time_interval"])
	}
}
