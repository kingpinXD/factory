package helper_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestCoordinatorOutputOptionalWorkSourcePreservesLegacyBytesAndPositiveReference(t *testing.T) {
	service, ref, _, _ := actualOutputPublicationCut(t, "receipt-before-ledger")
	prepared, _, proof, err := service.ReadControlOutputCommitLocal(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	caller := prepared.Caller.Coordinator
	if caller.WorkScope != nil || caller.WorkAuthority != nil {
		t.Fatal("legacy domain fixture gained inferred work source")
	}
	// These are the exact predecessor wire fields, before the omitted pointers existed.
	legacy := struct {
		Writer     controlrecord.LocalWriter    `json:"writer"`
		Authority  controlrecord.OwnerAuthority `json:"authority"`
		Control    effects.ControlReceiptRef    `json:"control"`
		Spec       session.Spec                 `json:"spec"`
		TargetID   string                       `json:"target_id"`
		OutputName string                       `json:"output_name"`
		Blueprint  controlrecord.ImmutableRef   `json:"blueprint"`
		Enrollment controlrecord.ImmutableRef   `json:"enrollment"`
	}{caller.Writer, caller.Authority, caller.Control, caller.Spec, caller.TargetID, caller.OutputName, caller.Blueprint, caller.Enrollment}
	old, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	now, err := json.Marshal(caller)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, now) || bytes.Contains(now, []byte("work_scope")) || bytes.Contains(now, []byte("work_authority")) {
		t.Fatal("omitted fields changed predecessor caller bytes")
	}
	original, err := controlrecord.ReadOriginal(service.Brain, prepared.Original)
	if err != nil {
		t.Fatal(err)
	}
	ingress := helper.ControlOutputIngress{Schema: 2, Source: helper.ControlIngressSource{Schema: 2, Original: prepared.Original, Control: caller.Control.Control, Input: effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256}, Authority: caller.Authority, Spec: caller.Spec, Writer: caller.Writer}, Target: store.Entry{Kind: caller.Authority.Kind, Epic: caller.Authority.EpicID, Dir: filepath.Dir(prepared.Path)}, TargetID: caller.TargetID, OutputName: caller.OutputName, Blueprint: caller.Blueprint}
	predecessor := struct {
		Schema     int                         `json:"schema"`
		Source     helper.ControlIngressSource `json:"source"`
		Target     store.Entry                 `json:"target"`
		TargetID   string                      `json:"target_id"`
		OutputName string                      `json:"output_name"`
		Blueprint  controlrecord.ImmutableRef  `json:"blueprint"`
	}{ingress.Schema, ingress.Source, ingress.Target, ingress.TargetID, ingress.OutputName, ingress.Blueprint}
	old, err = json.Marshal(predecessor)
	if err != nil {
		t.Fatal(err)
	}
	now, err = json.Marshal(ingress)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, now) {
		t.Fatal("omitted fields changed predecessor ingress bytes")
	}
	var decoded helper.ControlOutputIngress
	if err := toolcontract.DecodeBroker(old, &decoded); err != nil || decoded.WorkScope != nil || decoded.WorkAuthority != nil {
		t.Fatal("legacy ingress was not retained without inference", err)
	}
	oldPrepared, err := os.ReadFile(filepath.Join(service.Brain, "factory/helpers/control-outputs", ref.ID, "prepared.json"))
	if err != nil {
		t.Fatal(err)
	}
	oldCommit, err := os.ReadFile(proof.Path)
	if err != nil || effects.Digest(sha256.Sum256(oldCommit)) != proof.SHA256 {
		t.Fatal("getter did not return actual old commit-byte reference", err)
	}
	if err := service.AdoptControlOutputCommitLocked(context.Background(), ref, proof); err != nil {
		t.Fatal(err)
	}
	latest, err := os.ReadFile(filepath.Join(service.Brain, "factory/helpers/control-outputs", ref.ID, "prepared.json"))
	if err != nil || !bytes.Equal(latest, oldPrepared) {
		t.Fatal("positive adoption backfilled old prepared source", err)
	}
	latest, err = os.ReadFile(proof.Path)
	if err != nil || !bytes.Equal(latest, oldCommit) {
		t.Fatal("positive adoption rewrote old commit", err)
	}
	_, _, again, err := service.ReadControlOutputCommitLocal(context.Background(), ref)
	if err != nil || again != proof {
		t.Fatal("old positive reference changed", err)
	}
}
