package codex

import (
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type RegistryCheckReadRecipe struct {
	Version          int
	WorkAuthority    controlrecord.OwnerAuthority
	WorkInputsSHA256 effects.Digest
	Input            toolcontract.GitHubReadInput
}
