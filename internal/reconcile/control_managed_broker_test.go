package reconcile

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func managedBrokerOriginal(t *testing.T, w *world, request ControlReadRequest, call, op string, input any) controlrecord.OriginalRequestRef {
	t.Helper()
	caller := *request.Helper
	caller.CallID = call
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(toolcontract.Envelope{Op: op, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryHelper, wire, nil, &caller)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestManagedBrokerSourceChild(t *testing.T) {
	brain := os.Getenv("FACTORY_MANAGED_BROKER_SOURCE_CHILD")
	if brain == "" {
		return
	}
	var original controlrecord.OriginalRequestRef
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_MANAGED_BROKER_ORIGINAL")), &original); err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: brain, Authority: b2OfflineAuthority{NativeAuthority{brain}}}
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	witness, err := service.CaptureManagedControl(context.Background(), original)
	if err != nil {
		release()
		t.Fatal(err)
	}
	if os.Getenv("FACTORY_MANAGED_BROKER_CUT") == "after-commit" {
		if _, err := service.SubmitManagedControl(context.Background(), witness); err != nil {
			release()
			t.Fatal(err)
		}
	}
	release()
	bytes, _ := json.Marshal(witness)
	fmt.Fprintln(os.Stdout, string(bytes))
	io.Copy(io.Discard, os.Stdin)
}

func TestManagedBrokerActualDriverKillRecoversOnlyCommittedAdmission(t *testing.T) {
	for _, cut := range []string{"before-admit", "after-commit"} {
		t.Run(cut, func(t *testing.T) {
			w, service, parent, request := realHelperReadSourceFixture(t)
			original := managedBrokerOriginal(t, w, request, "managed-driver-"+cut, "submit", toolcontract.NestedSubmitInput{RequestKey: "actual-nested-driver", Component: "plan-reviewer", Prompt: "review actual source"})
			bytes, _ := json.Marshal(original)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(binary, "-test.run=^TestManagedBrokerSourceChild$")
			command.Env = append(os.Environ(), "FACTORY_MANAGED_BROKER_SOURCE_CHILD="+w.brain, "FACTORY_MANAGED_BROKER_ORIGINAL="+string(bytes), "FACTORY_MANAGED_BROKER_CUT="+cut)
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			command.Stderr = os.Stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				stdin.Close()
				if !waited {
					command.Process.Kill()
					command.Wait()
				}
			})
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() {
				t.Fatalf("receiving source child did not reach cut: %v", scanner.Err())
			}
			var witness controlrecord.ImmutableRef
			if err := json.Unmarshal(scanner.Bytes(), &witness); err != nil {
				t.Fatalf("invalid cut report %q: %v", scanner.Text(), err)
			}
			dir := filepath.Join(w.brain, "factory", "helpers", "control-admissions", "managed-receipts")
			if entries, err := os.ReadDir(dir); !errors.Is(err, os.ErrNotExist) && (err != nil || len(entries) != 0) {
				t.Fatalf("child published receipt before kill: %v", err)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			waited = true
			proof, receipt, err := service.LookupManagedControl(context.Background(), witness)
			jobs, jobErr := service.Jobs()
			if jobErr != nil {
				t.Fatal(jobErr)
			}
			if cut == "before-admit" {
				if !errors.Is(err, helper.ErrNotFound) || proof != (controlrecord.ImmutableRef{}) || len(jobs) != 1 {
					t.Fatalf("uncommitted request became positive fact: %+v %v jobs=%d", receipt, err, len(jobs))
				}
				return
			}
			if err != nil || receipt.Accepted == nil || receipt.ParentID != parent.ID || receipt.Accepted.JobID == parent.ID || len(jobs) != 2 {
				t.Fatalf("committed child not recovered: %+v %v jobs=%d", receipt, err, len(jobs))
			}
			reader := helper.Service{Brain: w.brain}
			again, retained, err := reader.LookupManagedControl(context.Background(), witness)
			if err != nil || again != proof || retained.Accepted == nil || *retained.Accepted != *receipt.Accepted {
				t.Fatalf("retained receipt required current parent: %+v %v", retained, err)
			}
			if _, err := helper.ReadManagedControlReceiptLocal(w.brain, proof); err != nil {
				t.Fatal(err)
			}
			jobs, err = service.Jobs()
			if err != nil || len(jobs) != 2 {
				t.Fatalf("lookup repeated nested submit: %v", err)
			}
		})
	}
}

func TestManagedBrokerActualParallelChildrenAndYieldReceipts(t *testing.T) {
	w, service, parent, request := realHelperReadSourceFixture(t)
	children := []string{}
	for _, key := range []string{"area-one", "area-two"} {
		original := managedBrokerOriginal(t, w, request, "managed-"+key, "submit", toolcontract.NestedSubmitInput{RequestKey: key, Component: "plan-reviewer", Prompt: key})
		witness, err := service.CaptureManagedControl(context.Background(), original)
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := service.SubmitManagedControl(context.Background(), witness)
		if err != nil {
			t.Fatal(err)
		}
		proof, receipt, err := service.LookupManagedControl(context.Background(), witness)
		if err != nil || receipt.Accepted == nil || receipt.Accepted.JobID != accepted.JobID || receipt.ParentID != parent.ID {
			t.Fatalf("child receipt wrong: %+v %v", receipt, err)
		}
		if _, err := helper.ReadManagedControlReceiptLocal(w.brain, proof); err != nil {
			t.Fatal(err)
		}
		children = append(children, accepted.JobID)
	}
	if children[0] == children[1] {
		t.Fatal("parallel same-role children collapsed")
	}
	collision := managedBrokerOriginal(t, w, request, "managed-collision", "submit", toolcontract.NestedSubmitInput{RequestKey: "area-one", Component: "plan-reviewer", Prompt: "different immutable payload"})
	witness, err := service.CaptureManagedControl(context.Background(), collision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SubmitManagedControl(context.Background(), witness); !errors.Is(err, helper.ErrConflict) {
		t.Fatalf("key conflict repeated admission: %v", err)
	}
	yieldOriginal := managedBrokerOriginal(t, w, request, "managed-yield", "yield", toolcontract.YieldInput{Children: children})
	witness, err = service.CaptureManagedControl(context.Background(), yieldOriginal)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.YieldManagedControl(context.Background(), witness); err != nil {
		t.Fatal(err)
	}
	proof, receipt, err := service.LookupManagedControl(context.Background(), witness)
	if err != nil || !receipt.Yielded || receipt.ParentID != parent.ID || receipt.Generation != parent.Generation || receipt.Round != parent.Round || len(receipt.Children) != 2 {
		t.Fatalf("yield receipt wrong: %+v %v", receipt, err)
	}
	if _, err := helper.ReadManagedControlReceiptLocal(w.brain, proof); err != nil {
		t.Fatal(err)
	}
}
