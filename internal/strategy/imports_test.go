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
// directly or transitively: they carry outcomes, history, faults, recording, or engine execution.
var forbidden = []string{
	"internal/history",
	"internal/oracle",
	"internal/exp",
	"internal/recovery",
	"internal/replay",
	"internal/faults",
	"internal/vfsrec",
	"internal/workload",
	"github.com/cockroachdb/pebble/v2",
}

func matchesForbidden(dep, banned string) bool {
	target := banned
	if strings.HasPrefix(banned, "internal/") {
		target = module + "/" + banned
	}
	return dep == target || strings.HasPrefix(dep, target+"/")
}

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
				if matchesForbidden(dep, banned) {
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
	// The detector itself must fire on a package that does reach recovery and pebble.
	ctrlCmd := exec.Command(tool, "list", "-deps", module+"/internal/recovery")
	ctrlCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	output, err := ctrlCmd.CombinedOutput()
	if err != nil {
		test.Fatalf("control listing failed: %v\n%s", err, output)
	}
	ctrlDeps := strings.Fields(string(output))
	sawRecovery, sawPebble := false, false
	for _, dep := range ctrlDeps {
		if matchesForbidden(dep, "internal/recovery") {
			sawRecovery = true
		}
		if matchesForbidden(dep, "github.com/cockroachdb/pebble/v2") {
			sawPebble = true
		}
	}
	if !sawRecovery || !sawPebble {
		test.Fatalf("control listing failed to detect internal/recovery (%v) and pebble (%v)", sawRecovery, sawPebble)
	}
}

var forbiddenIOPackages = map[string]bool{
	"os":            true,
	"io/ioutil":     true,
	"io/fs":         true,
	"path/filepath": true,
	"embed":         true,
	"net":           true,
	"net/http":      true,
	"os/exec":       true,
	"syscall":       true,
	"plugin":        true,
}

var forbiddenTransitiveStdPackages = map[string]bool{
	"embed":    true,
	"net":      true,
	"net/http": true,
	"os/exec":  true,
	"plugin":   true,
}

func findForbiddenIOImports(tool, pkg string) ([]string, error) {
	// Check non-standard-library packages in the transitive closure for any direct
	// I/O, filesystem, network, process execution, or dynamic plugin imports.
	cmdNonStd := exec.Command(tool, "list", "-deps", "-f", `{{if not .Standard}}{{.ImportPath}} {{join .Imports " "}}{{end}}`, module+"/"+pkg)
	cmdNonStd.Env = append(os.Environ(), "CGO_ENABLED=0")
	outNonStd, err := cmdNonStd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, line := range strings.Split(strings.TrimSpace(string(outNonStd)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		owner := fields[0]
		for _, imp := range fields[1:] {
			if forbiddenIOPackages[imp] {
				violations = append(violations, owner+" -> "+imp)
			}
		}
	}
	// Also verify none of the network/exec/embed/plugin stdlib packages appear anywhere transitively.
	cmdAll := exec.Command(tool, "list", "-deps", module+"/"+pkg)
	cmdAll.Env = append(os.Environ(), "CGO_ENABLED=0")
	outAll, err := cmdAll.CombinedOutput()
	if err != nil {
		return nil, err
	}
	for _, dep := range strings.Fields(string(outAll)) {
		if forbiddenTransitiveStdPackages[dep] {
			violations = append(violations, pkg+" (transitive) -> "+dep)
		}
	}
	return violations, nil
}

// TestPlanPackagesDoNotOpenFiles keeps PLAN packages from reading outcome
// artifacts through I/O: neither they nor any non-stdlib transitive dependency
// may import file, network, or exec packages.
func TestPlanPackagesDoNotOpenFiles(test *testing.T) {
	tool := goTool(test)
	for _, pkg := range planPackages {
		violations, err := findForbiddenIOImports(tool, pkg)
		if err != nil {
			test.Fatalf("go list imports %s: %v", pkg, err)
		}
		if len(violations) > 0 {
			test.Fatalf("%s has forbidden I/O/network/exec imports: %v", pkg, violations)
		}
	}
	// Positive control: internal/recovery uses os/time/pebble and must be flagged.
	ctrlViolations, err := findForbiddenIOImports(tool, "internal/recovery")
	if err != nil {
		test.Fatalf("control go list imports internal/recovery: %v", err)
	}
	if len(ctrlViolations) == 0 {
		test.Fatal("positive control failed: internal/recovery reported no forbidden I/O imports")
	}
}
