package strategy_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const module = "github.com/akshatrathore1/CraConTest"

var planPackages = []string{"internal/graph", "internal/candidate", "internal/strategy"}

// forbidden are the packages a PLAN-phase package must never depend on,
// directly or transitively: they carry outcomes, history or execution.
var forbidden = []string{"internal/history", "internal/oracle", "internal/exp", "internal/recovery", "internal/replay"}

func goTool(test *testing.T) string {
	test.Helper()
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	path := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(path); err != nil {
		test.Skip("go tool not available to run go list")
	}
	return path
}

// TestPlanImportBoundary inspects the actual transitive dependency list.
func TestPlanImportBoundary(test *testing.T) {
	tool := goTool(test)
	for _, pkg := range planPackages {
		command := exec.Command(tool, "list", "-deps", module+"/"+pkg)
		command.Env = append(os.Environ(), "CGO_ENABLED=0")
		output, err := command.CombinedOutput()
		if err != nil {
			test.Fatalf("go list -deps %s: %v\n%s", pkg, err, output)
		}
		deps := strings.Fields(string(output))
		if len(deps) < 2 || deps[len(deps)-1] != module+"/"+pkg {
			test.Fatalf("go list -deps %s returned an implausible list: %v", pkg, deps)
		}
		listed := make(map[string]bool, len(deps))
		for _, dep := range deps {
			listed[dep] = true
			for _, banned := range forbidden {
				if dep == module+"/"+banned || strings.HasPrefix(dep, module+"/"+banned+"/") {
					test.Fatalf("%s depends on %s", pkg, dep)
				}
			}
		}
		// Positive control: the list really is transitive, so an indirect
		// forbidden dependency would have appeared above.
		if !listed[module+"/internal/trace"] {
			test.Fatalf("go list -deps %s omitted internal/trace; the check would be vacuous", pkg)
		}
		if pkg == "internal/strategy" && (!listed[module+"/internal/graph"] || !listed[module+"/internal/candidate"]) {
			test.Fatal("strategy's dependency list is missing graph or candidate")
		}
	}
	// The detector itself must fire on a package that does reach recovery.
	output, err := exec.Command(tool, "list", "-deps", module+"/internal/recovery").CombinedOutput()
	if err != nil || !strings.Contains(string(output), module+"/internal/recovery") {
		test.Fatalf("control listing failed: %v\n%s", err, output)
	}
}

// TestPlanPackagesDoNotOpenFiles keeps PLAN packages from reading outcome
// artifacts through I/O: their non-test sources must not import os.
func TestPlanPackagesDoNotOpenFiles(test *testing.T) {
	tool := goTool(test)
	for _, pkg := range planPackages {
		command := exec.Command(tool, "list", "-f", "{{join .Imports \"\\n\"}}", module+"/"+pkg)
		output, err := command.CombinedOutput()
		if err != nil {
			test.Fatalf("go list imports %s: %v\n%s", pkg, err, output)
		}
		for _, imported := range strings.Fields(string(output)) {
			if imported == "os" || imported == "io/ioutil" || imported == "path/filepath" || imported == "net/http" {
				test.Fatalf("%s imports %s; PLAN packages must not read files", pkg, imported)
			}
		}
	}
}
