package exp

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// updateArtifacts controls whether benchmark matrix tests regenerate committed CSV
// artifacts in results/ (true) or validate existing committed files + run live smoke checks (false).
var updateArtifacts = flag.Bool("update", false, "regenerate committed benchmark CSV artifacts in results/")

func init() {
	if flag.Lookup("update-artifacts") == nil {
		flag.BoolVar(updateArtifacts, "update-artifacts", false, "alias for -update")
	}
}

func findResultsDirInternal(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err == nil {
		for {
			if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
				res := filepath.Join(dir, "results")
				if fi, resErr := os.Stat(res); resErr == nil && fi.IsDir() {
					return res
				}
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	fallback := filepath.Join("..", "..", "results")
	if fi, err := os.Stat(fallback); err == nil && fi.IsDir() {
		return fallback
	}
	return "results"
}
