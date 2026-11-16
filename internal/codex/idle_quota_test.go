package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostLoopIdleQuotaFailureWaitsForFreshCommandAdmission(t *testing.T) {
	for _, kind := range []string{"idle", "active", "compacting", "idle_healthy"} {
		t.Run(kind, func(t *testing.T) {
			state := kind
			healthy := kind == "idle_healthy"
			if healthy {
				state = "idle"
			}
			binary := filepath.Join(t.TempDir(), "factory")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			r := New(t.TempDir(), binary)
			r.Containment = &startupContainment{}
			if err := os.MkdirAll(filepath.Join(r.Brain, "factory"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(r.Brain, "factory/blueprint.yaml"), []byte("schema_version: 1\nvalues:\n  usage:\n    near_limit: 80\n    pause: 88\n    stale_after: 15m\n"), 0600); err != nil {
				t.Fatal(err)
			}
			record := fixtureRecord(t, r)
			record.State = state
			record.Selection.Profile.IdentityBinding = "fixture"
			inputRead, inputWrite := io.Pipe()
			outputRead, outputWrite := io.Pipe()
			p := newProtocol(inputWrite, outputRead)
			defer inputRead.Close()
			defer outputWrite.Close()
			defer p.Close()
			var quotaCalls, turnCalls atomic.Int32
			observed := make(chan struct{}, 8)
			go func() {
				dec := json.NewDecoder(inputRead)
				enc := json.NewEncoder(outputWrite)
				for {
					var call Message
					if dec.Decode(&call) != nil {
						return
					}
					if call.Method == "account/rateLimits/read" {
						quotaCalls.Add(1)
						observed <- struct{}{}
					} else if call.Method == "turn/start" {
						turnCalls.Add(1)
					}
					if call.Method == "account/read" {
						_ = enc.Encode(Message{ID: call.ID, Result: json.RawMessage(`{"account":{"type":"chatgpt"}}`)})
					} else if healthy && call.Method == "account/rateLimits/read" {
						data, _ := json.Marshal(map[string]any{"accountId": "fixture", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}, "rateLimitsByLimitId": map[string]any{"codex": map[string]any{"limitId": "codex", "spendControlReached": false, "primary": map[string]any{"usedPercent": 1, "windowDurationMins": 300, "resetsAt": time.Now().Add(time.Hour).Unix()}}}})
						_ = enc.Encode(Message{ID: call.ID, Result: data})
					} else if healthy && call.Method == "turn/start" {
						_ = enc.Encode(Message{ID: call.ID, Result: json.RawMessage(`{"turn":{"id":"next-turn"}}`)})
					} else {
						_ = enc.Encode(Message{ID: call.ID, Error: &RPCError{Code: -32603, Message: "fixture unavailable"}})
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ticks := make(chan time.Time)
			calls := make(chan hostCall)
			done := make(chan error, 1)
			stage := ""
			go func() {
				done <- r.runHostLoop(ctx, p, Owner{}, &record, calls, ticks, func() error { return nil }, &stage)
			}()
			select {
			case ticks <- time.Now():
			case err := <-done:
				t.Fatalf("loop ended before tick: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if state != "idle" {
				select {
				case err := <-done:
					var rpc *RPCError
					if !errors.As(err, &rpc) || rpc.Code != -32603 {
						t.Fatalf("active quota did not fail closed: %v", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if quotaCalls.Load() != 1 {
					t.Fatal("active quota not read")
				}
				return
			}
			// A synchronous host command proves the loop survived the idle tick.
			response := make(chan error, 1)
			select {
			case calls <- hostCall{request: hostRequest{Token: record.Token, Operation: "invalid-probe"}, response: response}:
			case err := <-done:
				t.Fatalf("idle quota read killed completed host: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-response:
			case err := <-done:
				t.Fatalf("idle host stopped: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if quotaCalls.Load() != 0 {
				t.Fatal("idle host polled account")
			}
			response = make(chan error, 1)
			calls <- hostCall{request: hostRequest{Token: record.Token, Operation: "deliver", Key: "next", Text: "next turn"}, response: response}
			if err := <-response; err != nil {
				t.Fatal(err)
			}
			select {
			case <-observed:
			case err := <-done:
				t.Fatalf("command failed before fresh quota: %v", err)
			case <-ctx.Done():
				<-done
				t.Fatalf("fresh admission not observed: hold=%q commands=%+v quota=%d", record.Hold, record.Commands, quotaCalls.Load())
			}
			// The next synchronous call serializes after queue admission.
			response = make(chan error, 1)
			select {
			case calls <- hostCall{request: hostRequest{Token: record.Token, Operation: "invalid-probe"}, response: response}:
			case err := <-done:
				t.Fatalf("unsubmitted command should stay queued: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			<-response
			wantTurns := int32(0)
			if healthy {
				wantTurns = 1
				if record.State != "active" || record.TurnID != "next-turn" || len(record.Commands) != 0 {
					t.Fatalf("healthy queued command did not settle: %+v", record.Commands)
				}
				response = make(chan error, 1)
				calls <- hostCall{request: hostRequest{Token: record.Token, Operation: "deliver", Key: "next", Text: "next turn"}, response: response}
				if err := <-response; err != nil {
					t.Fatal(err)
				}
			}
			if quotaCalls.Load() != 1 || turnCalls.Load() != wantTurns {
				t.Fatalf("fresh quota admission or duplicate dispatch failed: quota=%d turns=%d", quotaCalls.Load(), turnCalls.Load())
			}
			cancel()
			<-done
		})
	}
}
