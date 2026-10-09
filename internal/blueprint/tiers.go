package blueprint

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ProductClaude is the "Model tiers" column the factory reads its models from.
const ProductClaude = "Claude"

// Tiers maps a tier to its model per product: tiers["medium"]["Claude"] = "opus".
// A blank cell is an empty string: that product has no model for the tier.
type Tiers map[string]map[string]string

// ReadTiers parses the table under "## Model tiers" in <brain>/AGENTS.md.
func ReadTiers(brain string) (Tiers, error) {
	path := filepath.Join(brain, "AGENTS.md")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	tiers, err := parseTiers(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return tiers, nil
}

func parseTiers(data []byte) (Tiers, error) {
	var header []string
	tiers := Tiers{}
	inSection := false
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "## ") {
			if inSection {
				break
			}
			inSection = line == "## Model tiers"
			continue
		}
		if !inSection {
			continue
		}
		if !strings.HasPrefix(line, "|") {
			if header != nil {
				break
			}
			continue
		}
		cells := tableCells(line)
		switch {
		case header == nil:
			header = cells
		case strings.Trim(strings.Join(cells, ""), "-: ") == "":
			// the separator row
		default:
			row := map[string]string{}
			for i, product := range header[1:] {
				if i+1 < len(cells) {
					row[product] = cells[i+1]
				}
			}
			tiers[cells[0]] = row
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if !inSection {
		return nil, errors.New(`no "## Model tiers" section`)
	}
	if header == nil || len(tiers) == 0 {
		return nil, errors.New(`no table under "## Model tiers"`)
	}
	if header[0] != "Tier" || !slices.Contains(header[1:], ProductClaude) {
		return nil, fmt.Errorf("model tiers table header is %q, want Tier and a %s column", header, ProductClaude)
	}
	return tiers, nil
}

// Model returns tier's Claude model. It refuses an empty tier, a tier the
// table lacks, a tier with no Claude model and a denied model.
func (t Tiers) Model(tier string, deny []string) (string, error) {
	if tier == "" {
		return "", errors.New("the tier is empty")
	}
	row, ok := t[tier]
	if !ok {
		return "", fmt.Errorf("tier %q is not in the Model tiers table of AGENTS.md", tier)
	}
	model := row[ProductClaude]
	if model == "" {
		return "", fmt.Errorf("tier %q has no Claude model in AGENTS.md", tier)
	}
	if denied, ok := deniedModel(model, deny); ok {
		return "", fmt.Errorf("tier %q resolves to %q, which is denied (%s)", tier, model, denied)
	}
	return model, nil
}

func tableCells(line string) []string {
	parts := strings.Split(strings.Trim(line, "|"), "|")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}
