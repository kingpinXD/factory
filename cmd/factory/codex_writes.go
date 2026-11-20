package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type coordinatorWriteContext struct {
	check     func(context.Context) error
	bind      func(string, []byte) error
	completed []events.Event
}
type coordinatorWriteKey struct{}

type coordinatorWriteRunner struct {
	runner proc.Runner
	write  *coordinatorWriteContext
	brain  string
}

func (r coordinatorWriteRunner) Run(ctx context.Context, cmd proc.Cmd) ([]byte, error) {
	if err := r.write.check(ctx); err != nil {
		return nil, err
	}
	config, err := os.MkdirTemp(filepath.Join(r.brain, "factory"), "control-gh-config-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(config)
	cmd.Env = append(cmd.Env, "GH_CONFIG_DIR="+config)
	if data, err := readSecretsFile(filepath.Join(r.brain, "connectors", "secrets.env")); err == nil {
		if token := tokenIn(string(data)); token != "" {
			cmd.Env = append(cmd.Env, "GH_TOKEN="+token)
		}
	}
	return r.runner.Run(ctx, cmd)
}

func coordinatorProgramWrite(ctx context.Context, brain string, spec session.Spec, originalRef controlrecord.OriginalRequestRef, original controlrecord.OriginalRequest, control effects.ControlReceiptRef, input effects.ControlInput, owner reconcile.OwnerAuthority) (controlrecord.ImmutableRef, controlrecord.WriteResult, error) {
	if proof, result, err := controlrecord.FindWriteResult(brain, originalRef); err == nil || !errors.Is(err, os.ErrNotExist) {
		return proof, result, err
	}
	check := func(ctx context.Context) error {
		release, err := controlLock(ctx, brain)
		if err != nil {
			return err
		}
		defer release()
		if err := recheckControlV2(ctx, brain, *original.Control, spec); err != nil {
			return err
		}
		if err := reconcile.ValidateAdmittedOwnerAuthority(ctx, brain, owner); err != nil {
			return err
		}
		return helperService(brain).CheckWorkControlAdmission(ctx, input.Target.WorkID)
	}
	write := &coordinatorWriteContext{check: check, bind: func(key string, request []byte) error {
		return controlrecord.StoreWriteBinding(brain, controlrecord.WriteBinding{Original: originalRef, Key: key, RequestSHA256: sha256.Sum256(request)})
	}}
	ctx = context.WithValue(ctx, coordinatorWriteKey{}, write)
	value, err := scopedCoordinatorControl(ctx, brain, spec, controlRequest{Op: original.Operation, Payload: original.Payload})
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	if len(write.completed) == 0 {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, effects.ErrEvidence
	}
	reply, err := json.Marshal(value)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	result := controlrecord.WriteResult{Schema: 2, Original: originalRef, Control: control, Input: input, Owner: owner, Reply: reply, ReplySHA256: sha256.Sum256(reply)}
	for _, ev := range write.completed {
		encoded, _ := json.Marshal(ev)
		result.Domain = append(result.Domain, effects.EventRef{OwnerID: spec.Task, Sequence: ev.Seq, SHA256: sha256.Sum256(encoded)})
	}
	release, err := controlLock(ctx, brain)
	if err != nil {
		return controlrecord.ImmutableRef{}, result, err
	}
	defer release()
	if err := recheckControlV2(ctx, brain, *original.Control, spec); err != nil {
		return controlrecord.ImmutableRef{}, result, err
	}
	if err := reconcile.ValidateAdmittedOwnerAuthority(ctx, brain, owner); err != nil {
		return controlrecord.ImmutableRef{}, result, err
	}
	proof, err := controlrecord.StoreWriteResult(brain, result)
	return proof, result, err
}

func coordinatorPRCompleted(brain string, spec session.Spec, write *coordinatorWriteContext, key string) error {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil {
		return err
	}
	evs, err := events.Read(store.EventsPath(owner.Dir))
	if err != nil {
		return err
	}
	for _, prefix := range []string{"request:", "intent:", "done:"} {
		found := false
		for _, ev := range evs {
			if ev.Key == prefix+key {
				write.completed = append(write.completed, ev)
				found = true
				break
			}
		}
		if !found {
			return effects.ErrEvidence
		}
	}
	return nil
}
