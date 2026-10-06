package main

import (
	"testing"
	"time"

	"github.com/smartcontractkit/cre-sdk-go/capabilities/scheduler/cron"
	"github.com/smartcontractkit/cre-sdk-go/cre/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var anyExecutionTime = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

func TestInitWorkflow(t *testing.T) {
	config := &Config{
		Schedule: "0 */5 * * * *",
		Symbol:   "BTCUSDT",
	}
	runtime := testutils.NewRuntime(t, testutils.Secrets{})

	workflow, err := InitWorkflow(config, runtime.Logger(), nil)
	require.NoError(t, err)

	require.Len(t, workflow, 1)
	require.Equal(t, cron.Trigger(&cron.Config{}).CapabilityID(), workflow[0].CapabilityID())
}

func TestTechnicalPredictionLogic(t *testing.T) {
	config := &Config{
		Symbol:             "BTCUSDT",
		DefaultPositionUSD: 150.0,
		MaxLeverage:        5,
	}

	// Test oversold bounce
	pred := computeTechnicalPrediction(config, 85000.0, 0.5, 87000.0, 84800.0)
	require.Equal(t, "LONG", pred.Direction)
	require.Greater(t, pred.TargetPrice, 85000.0)
	require.Less(t, pred.StopLoss, 85000.0)
	require.Equal(t, 150.0, pred.PositionSizeUSD)
	require.GreaterOrEqual(t, pred.Confidence, 60)

	// Test overbought rejection
	predBear := computeTechnicalPrediction(config, 86800.0, -2.0, 87000.0, 84800.0)
	require.Equal(t, "SHORT", predBear.Direction)
	require.Less(t, predBear.TargetPrice, 86800.0)
	require.Greater(t, predBear.StopLoss, 86800.0)
}

func TestTriggerExecutionTime(t *testing.T) {
	payload := &cron.Payload{
		ScheduledExecutionTime: timestamppb.New(anyExecutionTime),
	}
	require.Equal(t, 2026, payload.ScheduledExecutionTime.AsTime().Year())
}
