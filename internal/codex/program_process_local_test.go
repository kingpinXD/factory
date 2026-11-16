package codex

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestProgramProcessLocalHistoryDoesNotClaimLiveGroupAbsence(t *testing.T) {
	brain := canonicalProcessBrain(t)
	identity, _ := ownSleep(t)
	record := closedProcessForTest(processRecordForTest(ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("3", 64)}, "execute"), identity)
	ref := writeProcessForTest(t, brain, record)
	local, err := ReadClosedProgramProcessLocal(context.Background(), brain, ref)
	if err != nil || local.ClaimID != record.ClaimID {
		t.Fatal("immutable historical evidence was not readable", err)
	}
	if _, err := ReadClosedProgramProcess(context.Background(), brain, ref); err == nil {
		t.Fatal("historical closed record falsely proved a live writer absent")
	}
	if alive, err := identity.Alive(); err != nil || !alive {
		t.Fatal("historical reader changed the live process", err)
	}
}

func TestProgramProcessLocalHistoryRejectsChangedReferencesAndStages(t *testing.T) {
	for _, change := range []string{"digest", "revision", "claim", "stage", "bytes", "alias"} {
		t.Run(change, func(t *testing.T) {
			brain := canonicalProcessBrain(t)
			record := closedProcessForTest(processRecordForTest(ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("4", 64)}, "execute"), ProcessIdentity{})
			ref := writeProcessForTest(t, brain, record)
			name, _ := ProgramProcessPath(brain, ref.SubjectID, ref.ClaimID)
			switch change {
			case "digest":
				ref.SHA256 = processDigest("changed")
			case "revision":
				ref.Revision++
			case "claim":
				ref.ClaimID = ref.SubjectID + "." + strings.Repeat("b", 32)
			case "stage":
				record.Stage, record.ClosedProof = "sealed", nil
				record.Revision--
				ref = writeProcessForTest(t, brain, record)
			case "bytes":
				if err := os.WriteFile(name, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "alias":
				if err := os.Rename(name, name+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(name+".old", name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ReadClosedProgramProcessLocal(context.Background(), brain, ref); err == nil {
				t.Fatal("invalid immutable physical reference accepted")
			}
			if _, err := ReadClosedProgramProcess(context.Background(), brain, ref); err == nil {
				t.Fatal("full reader accepted invalid immutable reference")
			}
		})
	}
}
