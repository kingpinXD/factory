package agents

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

var codexExplicitOperation = regexp.MustCompile(`\b(factory_control|factory_helper|managed_files|managed_terminal)\(op="([a-z_]+)"`)
var codexWrittenOperation = regexp.MustCompile(`factory_control operation ([a-z_]+)|"op"\s*:\s*"([a-z_]+)"|\b([a-z_]+)\{`)

// CheckCodexContractOperations checks operation instructions against the runtime catalogue.
func CheckCodexContractOperations(prompt string, helper bool) error {
	tool := toolcontract.FactoryControl
	if helper {
		tool = toolcontract.FactoryHelper
	}
	for _, call := range codexExplicitOperation.FindAllStringSubmatch(prompt, -1) {
		if !slices.Contains(toolcontract.OperationsForContract(call[1], 2), call[2]) {
			return fmt.Errorf("unavailable %s operation %s", call[1], call[2])
		}
	}
	for _, call := range codexWrittenOperation.FindAllStringSubmatch(prompt, -1) {
		op := call[1]
		if op == "" {
			op = call[2]
		}
		if op == "" {
			op = call[3]
			// Shorthand field objects name a known tool operation, rather than an envelope.
			known := slices.Contains(toolcontract.Operations(toolcontract.FactoryControl), op) || slices.Contains(toolcontract.Operations(toolcontract.FactoryHelper), op)
			if !known {
				continue
			}
		}
		if !slices.Contains(toolcontract.OperationsForContract(tool, 2), op) {
			return fmt.Errorf("unavailable %s operation %s", tool, op)
		}
	}
	return nil
}
