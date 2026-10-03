//go:build codex_integration

package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/coder/websocket"
)

type ephemeralIntegrationNamespaceObservation struct {
	marker, outer, self, init, procSelf string
	pid, ppid                           int
}

type ephemeralIntegrationNamespaceOwner struct{ identity string }

func integrationPIDNamespaceIdentity(value string) bool {
	if !strings.HasPrefix(value, "pid:[") || !strings.HasSuffix(value, "]") || len(value) > 32 {
		return false
	}
	number, err := strconv.ParseUint(value[5:len(value)-1], 10, 64)
	return err == nil && number != 0
}

func integrationNamespaceObservation() ephemeralIntegrationNamespaceObservation {
	self, _ := os.Readlink("/proc/self/ns/pid")
	init, _ := os.Readlink("/proc/1/ns/pid")
	procSelf, _ := os.Readlink("/proc/self")
	return ephemeralIntegrationNamespaceObservation{marker: os.Getenv("CODEX_EPHEMERAL_TEST_NAMESPACE"),
		outer: os.Getenv("CODEX_EPHEMERAL_TEST_OUTER_PID_NAMESPACE"), self: self, init: init, procSelf: procSelf,
		pid: syscall.Getpid(), ppid: syscall.Getppid()}
}

func integrationValidateNamespace(observation ephemeralIntegrationNamespaceObservation) (ephemeralIntegrationNamespaceOwner, error) {
	if observation.marker != "child" || observation.pid != 1 || observation.ppid != 0 ||
		!integrationPIDNamespaceIdentity(observation.outer) || !integrationPIDNamespaceIdentity(observation.self) ||
		observation.self == observation.outer || observation.init != observation.self || observation.procSelf != "1" {
		return ephemeralIntegrationNamespaceOwner{}, errors.New("private PID namespace ownership unproven")
	}
	return ephemeralIntegrationNamespaceOwner{identity: observation.self}, nil
}

func (owner ephemeralIntegrationNamespaceOwner) recheck(observation ephemeralIntegrationNamespaceObservation) error {
	current, err := integrationValidateNamespace(observation)
	if err != nil || owner.identity == "" || current.identity != owner.identity {
		return errors.New("private PID namespace ownership changed")
	}
	return nil
}

// The existing reexec itself is namespace init. Markers alone never authorize
// signals, and a fresh proc mount must describe this same private PID domain.
func integrationEnterNamespace(t *testing.T, selector string) (ephemeralIntegrationNamespaceOwner, bool) {
	t.Helper()
	if os.Getenv("CODEX_EPHEMERAL_TEST_NAMESPACE") != "child" {
		outer, err := os.Readlink("/proc/self/ns/pid")
		if err != nil || !integrationPIDNamespaceIdentity(outer) {
			t.Fatal("outer PID namespace prerequisite unavailable")
		}
		command := exec.Command(os.Args[0], "-test.run=^"+selector+"$", "-test.count=1", "-test.timeout=180s", "-test.v")
		command.Env = append(os.Environ(), "CODEX_EPHEMERAL_TEST_NAMESPACE=child", "CODEX_EPHEMERAL_TEST_OUTER_PID_NAMESPACE="+outer)
		command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID,
			UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Geteuid(), Size: 1}},
			GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getegid(), Size: 1}}, GidMappingsEnableSetgroups: false}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated fixture failed: %v\n%s", err, output)
		}
		t.Logf("isolated fixture completed\n%s", output)
		return ephemeralIntegrationNamespaceOwner{}, false
	}
	before := integrationNamespaceObservation()
	if before.pid != 1 || before.ppid != 0 || !integrationPIDNamespaceIdentity(before.outer) ||
		!integrationPIDNamespaceIdentity(before.self) || before.self == before.outer {
		t.Fatal("private PID namespace prerequisite unavailable")
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatal("private mount namespace unavailable")
	}
	if err := syscall.Mount("proc", "/proc", "proc", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
		t.Fatal("private PID proc mount unavailable")
	}
	owner, err := integrationValidateNamespace(integrationNamespaceObservation())
	if err != nil {
		t.Fatal(err)
	}
	return owner, true
}

type ephemeralIntegrationWait struct {
	done chan struct{}
	err  error // Published once, before done closes; readiness cannot consume it.
}

func integrationCommandWait(command *exec.Cmd) *ephemeralIntegrationWait {
	wait := &ephemeralIntegrationWait{done: make(chan struct{})}
	go func() { wait.err = command.Wait(); close(wait.done) }()
	return wait
}

func (wait *ephemeralIntegrationWait) result() (bool, error) {
	if wait == nil {
		return true, nil
	}
	select {
	case <-wait.done:
		return true, wait.err
	default:
		return false, nil
	}
}

func integrationExpectedStopResult(err error) bool {
	if err == nil {
		return true
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// These seams belong only to this finite namespace barrier's pure controls.
// No wildcard wait may compete with either command's process-specific Wait.
type ephemeralIntegrationNamespaceStopOps struct {
	observe func() ephemeralIntegrationNamespaceObservation
	kill    func() error
	reap    func() (int, error)
	now     func() time.Time
	tick    func(time.Time)
}

func integrationNamespaceStopOps() ephemeralIntegrationNamespaceStopOps {
	return ephemeralIntegrationNamespaceStopOps{
		observe: integrationNamespaceObservation,
		kill:    func() error { return syscall.Kill(-1, syscall.SIGKILL) },
		reap: func() (int, error) {
			var status syscall.WaitStatus
			return syscall.Wait4(-1, &status, syscall.WNOHANG|syscall.WALL, nil)
		},
		now: time.Now,
		tick: func(deadline time.Time) {
			remaining := time.Until(deadline)
			if remaining > 0 {
				timer := time.NewTimer(min(10*time.Millisecond, remaining))
				<-timer.C
			}
		},
	}
}

func integrationStopNamespace(owner ephemeralIntegrationNamespaceOwner, waits [2]*ephemeralIntegrationWait, ops ephemeralIntegrationNamespaceStopOps) error {
	deadline := ops.now().Add(2 * time.Second)
	for {
		if !ops.now().Before(deadline) {
			return errors.New("private PID namespace stop exceeded budget")
		}
		// Recheck before EVERY namespace-wide signal, including bounded retries.
		if err := owner.recheck(ops.observe()); err != nil {
			return err
		}
		if err := ops.kill(); err != nil && !errors.Is(err, syscall.ESRCH) {
			return errors.New("private PID namespace signal failed")
		}
		if !ops.now().Before(deadline) {
			return errors.New("private PID namespace stop exceeded budget")
		}
		finished := true
		for _, wait := range waits {
			done, err := wait.result()
			if done && !integrationExpectedStopResult(err) {
				return errors.New("direct native wait failed")
			}
			finished = finished && done
		}
		if finished {
			pid, err := ops.reap()
			if !ops.now().Before(deadline) {
				return errors.New("private PID namespace stop exceeded budget")
			}
			if errors.Is(err, syscall.ECHILD) {
				return nil
			}
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if err != nil || pid < 0 {
				return errors.New("private PID namespace reap failed")
			}
			if pid > 0 {
				continue
			}
			// WNOHANG zero proves children remain; elapsed time cannot prove empty.
		}
		ops.tick(deadline)
	}
}

func integrationNamespaceTestObservation() ephemeralIntegrationNamespaceObservation {
	return ephemeralIntegrationNamespaceObservation{marker: "child", outer: "pid:[100]", self: "pid:[200]", init: "pid:[200]", procSelf: "1", pid: 1, ppid: 0}
}

func TestEphemeralIntegrationNamespaceOwnershipGuard(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ephemeralIntegrationNamespaceObservation)
		owner  ephemeralIntegrationNamespaceOwner
	}{
		{"missing owner", nil, ephemeralIntegrationNamespaceOwner{}},
		{"marker only", func(o *ephemeralIntegrationNamespaceObservation) { o.pid = 20 }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"missing marker", func(o *ephemeralIntegrationNamespaceObservation) { o.marker = "" }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"parent PID nonzero", func(o *ephemeralIntegrationNamespaceObservation) { o.ppid = 20 }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"same outer namespace", func(o *ephemeralIntegrationNamespaceObservation) { o.outer = o.self }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"missing outer identity", func(o *ephemeralIntegrationNamespaceObservation) { o.outer = "" }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"malformed outer identity", func(o *ephemeralIntegrationNamespaceObservation) { o.outer = "pid:[zero]" }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"wrong init namespace", func(o *ephemeralIntegrationNamespaceObservation) { o.init = o.outer }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"stale proc", func(o *ephemeralIntegrationNamespaceObservation) { o.procSelf = "20" }, ephemeralIntegrationNamespaceOwner{"pid:[200]"}},
		{"changed owner namespace", nil, ephemeralIntegrationNamespaceOwner{"pid:[300]"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := integrationNamespaceTestObservation()
			if test.mutate != nil {
				test.mutate(&observation)
			}
			signals, reaps := 0, 0
			ops := ephemeralIntegrationNamespaceStopOps{
				observe: func() ephemeralIntegrationNamespaceObservation { return observation },
				kill:    func() error { signals++; return nil },
				reap:    func() (int, error) { reaps++; return -1, syscall.ECHILD },
				now:     time.Now, tick: func(time.Time) { t.Fatal("unguarded stop reached retry") },
			}
			if integrationStopNamespace(test.owner, [2]*ephemeralIntegrationWait{}, ops) == nil || signals != 0 || reaps != 0 {
				t.Fatal("unguarded namespace stop reached a syscall")
			}
		})
	}
}

func TestEphemeralIntegrationNamespaceStop(t *testing.T) {
	for _, name := range []string{"zero then ECHILD", "positive and EINTR then ECHILD", "ESRCH is not join", "signal error", "reap error", "direct wait error", "direct wait timeout", "direct owners before wildcard", "ownership changes during retry", "cached readiness result", "late ECHILD"} {
		t.Run(name, func(t *testing.T) {
			observation := integrationNamespaceTestObservation()
			owner, err := integrationValidateNamespace(observation)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(1, 0)
			waits := [2]*ephemeralIntegrationWait{{done: make(chan struct{})}, {done: make(chan struct{})}}
			pending := name == "direct owners before wildcard" || name == "direct wait timeout" || name == "ownership changes during retry"
			if !pending {
				close(waits[0].done)
				close(waits[1].done)
			}
			if name == "direct wait error" {
				waits[0].err = errors.New("synthetic unexpected wait failure")
			}
			if name == "cached readiness result" {
				for i := 0; i < 3; i++ {
					if done, err := waits[0].result(); !done || err != nil {
						t.Fatal("completion consumed")
					}
				}
			}
			signals, reaps, ticks := 0, 0, 0
			ops := ephemeralIntegrationNamespaceStopOps{
				observe: func() ephemeralIntegrationNamespaceObservation { return observation },
				kill: func() error {
					signals++
					if name == "signal error" {
						return syscall.EPERM
					}
					if name == "ESRCH is not join" {
						return syscall.ESRCH
					}
					return nil
				},
				reap: func() (int, error) {
					for _, wait := range waits {
						if done, _ := wait.result(); !done {
							t.Fatal("wildcard reap stole direct Wait")
						}
					}
					reaps++
					switch name {
					case "zero then ECHILD":
						if reaps == 1 {
							return 0, nil
						}
					case "positive and EINTR then ECHILD":
						if reaps == 1 {
							return 23, nil
						}
						if reaps == 2 {
							return -1, syscall.EINTR
						}
						if reaps == 3 {
							return 0, nil
						}
					case "ESRCH is not join":
						return 0, nil
					case "reap error":
						return -1, syscall.EINVAL
					case "late ECHILD":
						now = now.Add(3 * time.Second)
					}
					return -1, syscall.ECHILD
				},
				now: func() time.Time { return now },
				tick: func(deadline time.Time) {
					ticks++
					if !deadline.Equal(time.Unix(1, 0).Add(2 * time.Second)) {
						t.Fatal("stop deadline extended")
					}
					now = now.Add(500 * time.Millisecond)
					if name == "direct owners before wildcard" {
						close(waits[ticks-1].done)
					}
					if name == "ownership changes during retry" {
						observation.self, observation.init = "pid:[300]", "pid:[300]"
					}
				},
			}
			err = integrationStopNamespace(owner, waits, ops)
			wantError := name == "ESRCH is not join" || name == "signal error" || name == "reap error" || name == "direct wait error" || name == "direct wait timeout" || name == "ownership changes during retry" || name == "late ECHILD"
			if (err != nil) != wantError {
				t.Fatal("wrong stop result")
			}
			if (name == "direct wait error" || name == "direct wait timeout" || name == "ownership changes during retry") && reaps != 0 {
				t.Fatal("wildcard reaped before direct owners")
			}
			if name == "direct owners before wildcard" && (signals != 3 || ticks != 2 || reaps != 1) {
				t.Fatal("pending direct owners were not repeatedly signalled first")
			}
			if name == "zero then ECHILD" && (reaps != 2 || ticks != 1) {
				t.Fatal("zero treated as empty")
			}
			if name == "positive and EINTR then ECHILD" && reaps != 4 {
				t.Fatal("reap results were not drained")
			}
		})
	}
}

// This distinct post-review selector never runs under the pure
// ^TestEphemeralIntegration prefix. It needs namespaces, but no model/binary.
func TestEphemeralNamespaceOwnershipIntegration(t *testing.T) {
	owner, child := integrationEnterNamespace(t, "TestEphemeralNamespaceOwnershipIntegration")
	if !child {
		return
	}
	for cycle := 0; cycle < 2; cycle++ {
		root := t.TempDir()
		var waits [2]*ephemeralIntegrationWait
		for index, role := range []string{"leader", "orphan-parent"} {
			if owner.recheck(integrationNamespaceObservation()) != nil {
				t.Fatal("helper start lost namespace ownership")
			}
			command := integrationNamespaceHelperCommand(root, role, owner.identity)
			if command.Start() != nil {
				t.Fatal("namespace ownership helper launch failed")
			}
			waits[index] = integrationCommandWait(command)
		}
		deadline := time.Now().Add(3 * time.Second)
		ready := false
		for time.Now().Before(deadline) {
			ready = true
			for _, name := range []string{"leader", "detached", "orphan-parent", "orphan-adopted"} {
				data, err := os.ReadFile(filepath.Join(root, name))
				ready = ready && err == nil && string(data) == "ready\n"
			}
			parentDone, parentErr := waits[1].result()
			if parentDone && parentErr != nil {
				t.Fatal("orphan parent helper failed")
			}
			if ready && parentDone {
				break
			}
			ready = false
			timer := time.NewTimer(10 * time.Millisecond)
			<-timer.C
		}
		if !ready {
			t.Fatal("detached/orphan ownership readiness missing")
		}
		if done, _ := waits[0].result(); done {
			t.Fatal("live leader exited before stop")
		}
		ops := integrationNamespaceStopOps()
		reap := ops.reap
		adopted := 0
		ops.reap = func() (int, error) {
			pid, err := reap()
			if pid > 0 {
				adopted++
			}
			return pid, err
		}
		if err := integrationStopNamespace(owner, waits, ops); err != nil {
			t.Fatal(err)
		}
		if adopted < 2 {
			t.Fatal("detached and orphan descendants were not reaped")
		}
		for _, wait := range waits {
			if done, err := wait.result(); !done || !integrationExpectedStopResult(err) {
				t.Fatal("direct helper completion missing")
			}
		}
		if owner.recheck(integrationNamespaceObservation()) != nil {
			t.Fatal("namespace init did not survive stop")
		}
		t.Logf("namespace cycle=%d directWaits=2 adoptedReaped=%d empty=ECHILD initAlive=true", cycle+1, adopted)
	}
}

func integrationNamespaceHelperCommand(root, role, identity string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestEphemeralNamespaceOwnedHelper$", "-test.count=1", "-test.timeout=40s")
	command.Env = append(os.Environ(), "CODEX_EPHEMERAL_TEST_OWNERSHIP_HELPER="+role,
		"CODEX_EPHEMERAL_TEST_OWNERSHIP_ROOT="+root, "CODEX_EPHEMERAL_TEST_OWNERSHIP_PID_NAMESPACE="+identity,
		"GORACE=atexit_sleep_ms=0")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	return command
}

// Only the private ownership control can invoke these finite helper roles.
// setsid deliberately breaks leader-group ownership without leaving the PID
// namespace. The orphan-parent deliberately returns before its child does.
func TestEphemeralNamespaceOwnedHelper(t *testing.T) {
	role := os.Getenv("CODEX_EPHEMERAL_TEST_OWNERSHIP_HELPER")
	root := os.Getenv("CODEX_EPHEMERAL_TEST_OWNERSHIP_ROOT")
	identity, _ := os.Readlink("/proc/self/ns/pid")
	outer := os.Getenv("CODEX_EPHEMERAL_TEST_OUTER_PID_NAMESPACE")
	if os.Getenv("CODEX_EPHEMERAL_TEST_NAMESPACE") != "child" || syscall.Getpid() <= 1 || syscall.Getppid() <= 0 ||
		!integrationPIDNamespaceIdentity(identity) || identity == outer || !integrationPIDNamespaceIdentity(outer) ||
		identity != os.Getenv("CODEX_EPHEMERAL_TEST_OWNERSHIP_PID_NAMESPACE") || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		t.Fatal("private namespace helper prerequisites missing")
	}
	if role != "leader" && role != "orphan-parent" && role != "detached" && role != "orphan" {
		t.Fatal("unknown namespace helper role")
	}
	ready := func(name string) {
		if os.WriteFile(filepath.Join(root, name), []byte("ready\n"), 0600) != nil {
			t.Fatal("namespace helper readiness failed")
		}
	}
	if role == "leader" || role == "orphan-parent" {
		childRole := "detached"
		if role == "orphan-parent" {
			childRole = "orphan"
		}
		command := integrationNamespaceHelperCommand(root, childRole, identity)
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if command.Start() != nil {
			t.Fatal("detached namespace helper launch failed")
		}
		ready(role)
		if role == "orphan-parent" {
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if data, err := os.ReadFile(filepath.Join(root, "orphan-started")); err == nil && string(data) == "ready\n" {
					return
				}
				timer := time.NewTimer(10 * time.Millisecond)
				<-timer.C
			}
			t.Fatal("orphan child startup missing")
		}
	} else if role == "orphan" {
		ready("orphan-started")
		deadline := time.Now().Add(3 * time.Second)
		for syscall.Getppid() != 1 && time.Now().Before(deadline) {
			timer := time.NewTimer(10 * time.Millisecond)
			<-timer.C
		}
		if syscall.Getppid() != 1 {
			t.Fatal("orphan was not adopted by namespace init")
		}
		ready("orphan-adopted")
	} else {
		if group, err := syscall.Getpgid(0); err != nil || group != syscall.Getpid() {
			t.Fatal("detached helper has no independent session group")
		}
		ready("detached")
	}
	// Hold the real helper alive until the owner's namespace SIGKILL. Expiry is
	// a failure, never a substitute for the owner's two-second join predicate.
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	<-timer.C
	t.Fatal("namespace helper outlived its stop control")
}

// This entry point is intentionally absent from quick checks. Missing exact
// candidate/hash, Linux namespace/firewall tools or schema/source prerequisites
// FAIL the explicit invocation. No installed serving instance is contacted.
// Native builtin OpenAI HTTP-fallback coverage exercises HTTP/SSE after a real
// upgrade refusal. It does not cover successful WebSocket framing, continuation
// caching, reconnects or hostile delivery specifically over WebSockets.
func TestEphemeralProtocolIntegration(t *testing.T) {
	binary := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_BINARY")
	expected := os.Getenv("CODEX_EPHEMERAL_TEST_SHA256")
	file, err := os.Open(binary)
	if err != nil {
		t.Fatal("candidate binary unavailable")
	}
	digest := sha256.New()
	_, hashErr := io.Copy(digest, file)
	_ = file.Close()
	if hashErr != nil {
		t.Fatal("candidate binary hash failed")
	}
	if len(expected) != 64 || hex.EncodeToString(digest.Sum(nil)) != expected {
		t.Fatal("candidate differs from reviewed CODEX_EPHEMERAL_TEST_SHA256")
	}
	source := os.Getenv("CODEX_EPHEMERAL_TEST_SOURCE")
	if !filepath.IsAbs(source) {
		t.Fatal("CODEX_EPHEMERAL_TEST_SOURCE must name the exact candidate codex-rs source")
	}
	for _, path := range []string{"core/config.schema.json", "models-manager/models.json"} {
		if _, err := os.Stat(filepath.Join(source, path)); err != nil {
			t.Fatalf("candidate source prerequisite missing: %s", path)
		}
	}
	metadata, err := os.ReadFile(filepath.Join(source, "models-manager/models.json"))
	if err != nil || len(metadata) > 4*1024*1024 {
		t.Fatal("candidate model metadata unavailable")
	}
	var catalog struct {
		Models []struct {
			Slug     string   `json:"slug"`
			Tools    []string `json:"experimental_supported_tools"`
			ToolMode *string  `json:"tool_mode"`
			Lite     bool     `json:"use_responses_lite"`
			Efforts  []struct {
				Effort string `json:"effort"`
			} `json:"supported_reasoning_levels"`
		} `json:"models"`
	}
	if json.Unmarshal(metadata, &catalog) != nil {
		t.Fatal("candidate model metadata invalid")
	}
	unchanged := false
	for _, model := range catalog.Models {
		if model.Slug == "gpt-5.5" {
			low := false
			for _, effort := range model.Efforts {
				if effort.Effort == "low" {
					low = true
				}
			}
			unchanged = low && model.ToolMode == nil && !model.Lite && len(model.Tools) == 0
		}
	}
	if !unchanged || !validEphemeralCatalog(filepath.Join(source, "models-manager/models.json")) {
		t.Fatal("candidate must retain original gpt-5.5 Direct/non-Lite metadata")
	}
	ip := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_IP")
	nft := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_NFT")
	_ = integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
	owner, child := integrationEnterNamespace(t, "TestEphemeralProtocolIntegration")
	if !child {
		return
	}
	if err := syscall.Mount("tmpfs", "/etc", "tmpfs", syscall.MS_NODEV|syscall.MS_NOSUID, "mode=0755"); err != nil {
		t.Fatal("disposable system requirements mount unavailable")
	}
	if output, err := exec.Command(ip, "link", "set", "lo", "up").CombinedOutput(); err != nil {
		t.Fatalf("cannot configure isolated loopback: %v %s", err, output)
	}
	harness := newEphemeralIntegrationHarness(t, binary, nft, owner)
	defer harness.close()
	harness.control(t)
	harness.catalogControls(t)
	for _, name := range []string{"baseline", "static-refresh", "server-question-control", "early-events", "lost-turn", "cancel-start", "disconnect", "ambiguous", "stale", "malformed", "missing-final", "timeout-config", "timeout-model", "timeout", "question-at-cancel"} {
		t.Run(name, func(t *testing.T) { harness.run(t, name, "", "") })
	}
	t.Run("after static refresh", func(t *testing.T) { harness.run(t, "baseline", "", "") })
	// The router must reject these despite their absence from advertised tools.
	// Namespace and custom calls exercise dispatch, not a prompt prohibition.
	hostile := []struct{ name, namespace, arguments string }{
		{"exec_command", "", `{"cmd":"printf changed > CANARY","yield_time_ms":100}`},
		{"shell", "", `{"command":["sh","-c","cat CANARY"]}`},
		{"apply_patch", "", `*** Begin Patch\n*** Add File: CANARY\n+changed\n*** End Patch`},
		{"view_image", "", `{"path":"CANARY"}`},
		{"run", "web", `{"open":[{"ref_id":"CANARY_URL"}]}`},
		{"imagegen", "image_gen", `{"prompt":"synthetic","referenced_image_paths":["CANARY"]}`},
		{"echo", "mcp__literal.name", `{"value":"CANARY"}`},
		{"read_resource", "mcp", `{"uri":"CANARY"}`},
		{"search", "codex_apps", `{"query":"synthetic"}`},
		{"tool_search", "", `{"query":"shell"}`},
		{"exec", "functions", `await tools.exec_command({cmd:"cat CANARY"})`},
		{"spawn_agent", "", `{"message":"read CANARY"}`},
		{"search", "memories", `{"query":"synthetic"}`},
		{"create_goal", "", `{"objective":"read CANARY"}`},
		{"request_plugin_install", "", `{"plugin_id":"synthetic@fixture","suggest_reason":"synthetic"}`},
		{"list", "skills", `{}`},
		{"wait", "functions", `{"cell_id":"synthetic"}`},
		{"curr_time", "clock", `{}`},
		{"request_user_input_async", "", `{"questions":[{"title":"Synthetic?","options":["Yes","No"]}]}`},
		{"read", "skills", `{"path":"CANARY"}`},
	}
	for _, attack := range hostile {
		t.Run("forbidden/"+attack.namespace+"/"+attack.name, func(t *testing.T) { harness.run(t, "forbidden", attack.namespace, attack.name+"\n"+attack.arguments) })
	}
	// Reconfiguration is between calls, never an arbitrary mid-call admin race.
	harness.reconfigure(t, "fresh.literal.name")
	t.Run("fresh inventory", func(t *testing.T) { harness.run(t, "baseline", "", "") })
	harness.managedConflict(t)
	harness.verifyPersistence(t)
	if harness.droppedPackets(t) != harness.egressDrops {
		t.Fatal("final fixed deny baseline changed")
	}
}

func integrationExecutable(t *testing.T, key string) string {
	t.Helper()
	path := os.Getenv(key)
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must name an explicit absolute executable", key)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("%s executable unavailable", key)
	}
	return path
}

// Keep only the final 8 KiB of this disposable process's synthetic diagnostics.
type ephemeralIntegrationStderr struct {
	mu   sync.Mutex
	tail []byte
}

func (buffer *ephemeralIntegrationStderr) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	count := len(data)
	if buffer.tail == nil {
		buffer.tail = make([]byte, 0, 8192)
	}
	if len(data) > 8192 {
		data = data[len(data)-8192:]
	}
	if excess := len(buffer.tail) + len(data) - 8192; excess > 0 {
		copy(buffer.tail, buffer.tail[excess:])
		buffer.tail = buffer.tail[:len(buffer.tail)-excess]
	}
	buffer.tail = append(buffer.tail, data...)
	return count, nil
}

func (buffer *ephemeralIntegrationStderr) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return string(buffer.tail)
}

type ephemeralIntegrationHTTPRequest struct {
	Method           string `json:"method"`
	Path             string `json:"path"`
	ContentEncoding  string `json:"contentEncoding"`
	Connection       string `json:"connection,omitempty"`
	Upgrade          string `json:"upgrade,omitempty"`
	WebSocketVersion string `json:"webSocketVersion,omitempty"`
	UpgradeAccepted  bool   `json:"upgradeAccepted,omitempty"`
	Error            string `json:"error,omitempty"`
}

func integrationDiagnosticText(value string, limit int) string {
	return value[:min(len(value), limit)]
}

const integrationControlInput = "ordinary synthetic control"

var integrationControlSentinels = [...]string{
	"GLOBAL_POLICY_SENTINEL", "PROJECT_POLICY_SENTINEL", "WORKSPACE_POLICY_SENTINEL", "TEAM_POLICY_SENTINEL",
	"SKILL_POLICY_SENTINEL", "PLUGIN_SKILL_SENTINEL", "INHERITED_FILE_POLICY_SENTINEL", "HOST_HOOK_SENTINEL",
}

func integrationControlRequest(request map[string]any, threadID, turnID string) bool {
	metadata, _ := request["client_metadata"].(map[string]any)
	if threadID == "" || turnID == "" || metadata["thread_id"] != threadID || metadata["turn_id"] != turnID {
		return false
	}
	input, _ := request["input"].([]any)
	for _, value := range input {
		item, _ := value.(map[string]any)
		if item["type"] != "message" || item["role"] != "user" {
			continue
		}
		content, _ := item["content"].([]any)
		for _, value := range content {
			part, _ := value.(map[string]any)
			if part["type"] == "input_text" && part["text"] == integrationControlInput {
				return true
			}
		}
	}
	return false
}

func integrationControlRequestDiagnostic(request map[string]any, threadID, turnID string) map[string]any {
	encoded, _ := json.Marshal(request)
	present := map[string]bool{}
	for _, sentinel := range integrationControlSentinels {
		present[sentinel] = bytes.Contains(encoded, []byte(sentinel))
	}
	metadata, _ := request["client_metadata"].(map[string]any)
	reasoning, _ := request["reasoning"].(map[string]any)
	var turnMetadata struct {
		RequestKind string `json:"request_kind"`
	}
	if raw := stringValue(metadata["x-codex-turn-metadata"]); len(raw) <= 16384 {
		_ = json.Unmarshal([]byte(raw), &turnMetadata)
	}
	input, _ := request["input"].([]any)
	shape := []string{}
	for _, value := range input[:min(len(input), 8)] {
		item, _ := value.(map[string]any)
		shape = append(shape, integrationDiagnosticText(stringValue(item["type"]), 32)+"/"+integrationDiagnosticText(stringValue(item["role"]), 32))
	}
	instructions, _ := request["instructions"].(string)
	tools, _ := request["tools"].([]any)
	return map[string]any{
		"model":                 integrationDiagnosticText(stringValue(request["model"]), 128),
		"effort":                integrationDiagnosticText(stringValue(reasoning["effort"]), 32),
		"threadMatches":         threadID != "" && metadata["thread_id"] == threadID,
		"turnMatches":           turnID != "" && metadata["turn_id"] == turnID,
		"requestKind":           integrationDiagnosticText(turnMetadata.RequestKind, 32),
		"representativeControl": integrationControlRequest(request, threadID, turnID),
		"sentinels":             present, "inputItems": len(input), "inputShape": shape,
		"instructionsBytes": len(instructions), "topLevelTools": len(tools),
	}
}

func (h *ephemeralIntegrationHarness) controlDiagnostic(threadID, turnID string, sources []string) string {
	// Synthetic shape/source evidence only: never log the model request, tool
	// arguments, instruction text, authorization headers or credential contents.
	h.mu.Lock()
	requestCount := len(h.requests)
	requests := append([]map[string]any(nil), h.requests[:min(len(h.requests), 8)]...)
	httpRequests := make([]ephemeralIntegrationHTTPRequest, 0, min(len(h.httpRequests), 8))
	for _, request := range h.httpRequests[:min(len(h.httpRequests), 8)] {
		httpRequests = append(httpRequests, *request)
	}
	h.mu.Unlock()
	shapes := []map[string]any{}
	for _, request := range requests {
		shapes = append(shapes, integrationControlRequestDiagnostic(request, threadID, turnID))
	}
	boundedSources := []string{}
	for _, source := range sources[:min(len(sources), 16)] {
		boundedSources = append(boundedSources, integrationDiagnosticText(source, 512))
	}
	encoded, _ := json.Marshal(struct {
		Requests               []map[string]any                  `json:"requests"`
		DecodedPOSTCount       int                               `json:"decodedPOSTCount"`
		InstructionSources     []string                          `json:"instructionSources"`
		InstructionSourceCount int                               `json:"instructionSourceCount"`
		HTTP                   []ephemeralIntegrationHTTPRequest `json:"http"`
	}{shapes, requestCount, boundedSources, len(sources), httpRequests})
	return integrationDiagnosticText(string(encoded), 8192)
}

func TestEphemeralIntegrationControlRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
		valid  bool
	}{
		{"representative turn", nil, true},
		{"foreign thread", func(r map[string]any) { r["client_metadata"].(map[string]any)["thread_id"] = "other" }, false},
		{"foreign turn", func(r map[string]any) { r["client_metadata"].(map[string]any)["turn_id"] = "other" }, false},
		{"missing identity", func(r map[string]any) { delete(r, "client_metadata") }, false},
		{"startup without input", func(r map[string]any) { r["input"] = []any{} }, false},
		{"different input", func(r map[string]any) {
			r["input"].([]any)[0].(map[string]any)["content"] = []any{map[string]any{"type": "input_text", "text": "background request"}}
		}, false},
		{"developer mentions control", func(r map[string]any) { r["input"].([]any)[0].(map[string]any)["role"] = "developer" }, false},
		{"tool output mentions control", func(r map[string]any) {
			r["input"] = []any{map[string]any{"type": "function_call_output", "output": integrationControlInput}}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{"client_metadata": map[string]any{"thread_id": "thread", "turn_id": "turn"},
				"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": integrationControlInput}}}}}
			if test.mutate != nil {
				test.mutate(request)
			}
			if integrationControlRequest(request, "thread", "turn") != test.valid || integrationControlRequest(request, "", "") {
				t.Fatal("background or unowned request changed representative control eligibility")
			}
		})
	}
	t.Run("bounded synthetic diagnostics", func(t *testing.T) {
		privateText := "SYNTHETIC_TEXT_MUST_NOT_BE_LOGGED"
		harness := &ephemeralIntegrationHarness{}
		for index := 0; index < 64; index++ {
			harness.requests = append(harness.requests, map[string]any{"model": strings.Repeat("m", 1024),
				"instructions": privateText + integrationControlSentinels[0], "input": []any{map[string]any{"type": "message", "role": "user", "content": privateText}}})
		}
		sources := make([]string, 32)
		for index := range sources {
			sources[index] = strings.Repeat("p", 1024)
		}
		diagnostic := harness.controlDiagnostic("thread", "turn", sources)
		shape := integrationControlRequestDiagnostic(harness.requests[0], "thread", "turn")
		if len(diagnostic) > 8192 || strings.Contains(diagnostic, privateText) || !strings.Contains(diagnostic, "\"sentinels\"") || len(shape["model"].(string)) != 128 ||
			len(shape["sentinels"].(map[string]bool)) != 8 || !shape["sentinels"].(map[string]bool)[integrationControlSentinels[0]] {
			t.Fatal("synthetic request diagnostics lost sentinel evidence or leaked full input")
		}
	})
}

func integrationWebSocketUpgrade(request *http.Request) bool {
	if request.Method != http.MethodGet || request.URL.Path != "/v1/responses" {
		return false
	}
	upgrade := request.Header.Values("Upgrade")
	version := request.Header.Values("Sec-WebSocket-Version")
	keys := request.Header.Values("Sec-WebSocket-Key")
	if len(upgrade) != 1 || !strings.EqualFold(strings.TrimSpace(upgrade[0]), "websocket") ||
		len(version) != 1 || strings.TrimSpace(version[0]) != "13" || len(keys) != 1 {
		return false
	}
	connectionUpgrade := false
	for _, value := range request.Header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				connectionUpgrade = true
			}
		}
	}
	key := strings.TrimSpace(keys[0])
	if len(key) != 24 {
		return false
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(key)
	return connectionUpgrade && err == nil && len(decoded) == 16
}

func TestEphemeralIntegrationHTTPFallbackResponder(t *testing.T) {
	for _, test := range []struct {
		name           string
		mutate         func(*http.Request)
		control, valid bool
	}{
		{"control upgrade", nil, true, true},
		{"case insensitive tokens", func(r *http.Request) {
			r.Header.Set("Connection", "keep-alive, UpGrAdE")
			r.Header.Set("Upgrade", "WebSocket")
		}, false, true},
		{"multiple connection values", func(r *http.Request) {
			r.Header.Set("Connection", "keep-alive")
			r.Header.Add("Connection", "upgrade")
		}, false, true},
		{"plain GET", func(r *http.Request) { r.Header = http.Header{} }, false, false},
		{"missing connection", func(r *http.Request) { r.Header.Del("Connection") }, false, false},
		{"connection substring", func(r *http.Request) { r.Header.Set("Connection", "notupgrade") }, false, false},
		{"missing upgrade", func(r *http.Request) { r.Header.Del("Upgrade") }, false, false},
		{"different upgrade", func(r *http.Request) { r.Header.Set("Upgrade", "h2c") }, false, false},
		{"duplicate upgrade", func(r *http.Request) { r.Header.Add("Upgrade", "websocket") }, false, false},
		{"missing version", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Version") }, false, false},
		{"wrong version", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "12") }, false, false},
		{"duplicate version", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Version", "13") }, false, false},
		{"missing key", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") }, false, false},
		{"malformed key", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", strings.Repeat("!", 24)) }, false, false},
		{"short decoded key", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 15)))
		}, false, false},
		{"long decoded key", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 17)))
		}, false, false},
		{"duplicate key", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") }, false, false},
		{"unrelated path", func(r *http.Request) { r.URL.Path = "/unexpected" }, false, false},
		{"unrelated method", func(r *http.Request) { r.Method = http.MethodPut }, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := &ephemeralIntegrationHarness{controlMode: test.control}
			request := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			request.Header.Set("Connection", "upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			if test.mutate != nil {
				test.mutate(request)
			}
			response := httptest.NewRecorder()
			harness.respond(response, request)
			want := http.StatusNotFound
			if test.valid {
				want = http.StatusUpgradeRequired
			}
			if response.Code != want {
				t.Fatalf("HTTP response=%d, want %d", response.Code, want)
			}
			if len(harness.requests) != 0 || len(harness.httpRequests) != 1 || harness.httpRequests[0].UpgradeAccepted != test.valid {
				t.Fatal("upgrade evidence changed decoded Responses eligibility")
			}
			control, utility := 0, 0
			if test.valid && test.control {
				control = 1
			} else if test.valid {
				utility = 1
			}
			if harness.controlUpgrades != control || harness.utilityUpgrades != utility {
				t.Fatal("upgrade evidence attributed to the wrong fixture call")
			}
		})
	}
	t.Run("bounded upgrade evidence", func(t *testing.T) {
		harness := &ephemeralIntegrationHarness{}
		for _, control := range []bool{true, false} {
			harness.controlMode = control
			for index := 0; index < 70; index++ {
				request := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
				request.Header.Set("Connection", strings.Repeat("keep-alive,", 64)+"upgrade")
				request.Header.Set("Upgrade", "websocket")
				request.Header.Set("Sec-WebSocket-Version", "13")
				request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				response := httptest.NewRecorder()
				harness.respond(response, request)
				if response.Code != http.StatusUpgradeRequired {
					t.Fatal("diagnostic saturation changed valid handshake refusal")
				}
			}
		}
		if harness.controlUpgrades != 64 || harness.utilityUpgrades != 64 || len(harness.httpRequests) != 64 ||
			len(harness.httpRequests[0].Connection) > 128 || len(harness.requests) != 0 {
			t.Fatal("upgrade evidence exceeded its bounds or admitted Responses")
		}
	})
}

func TestEphemeralIntegrationDiagnosticBounds(t *testing.T) {
	t.Run("stderr tail", func(t *testing.T) {
		for _, chunks := range [][]string{{"short"}, {strings.Repeat("a", 8192), "tail"}, {"old", strings.Repeat("b", 16384)}, {""}} {
			var buffer ephemeralIntegrationStderr
			var complete string
			for _, chunk := range chunks {
				if count, err := buffer.Write([]byte(chunk)); err != nil || count != len(chunk) {
					t.Fatalf("stderr write=%d %v", count, err)
				}
				complete += chunk
			}
			want := complete[max(0, len(complete)-8192):]
			if buffer.String() != want || len(buffer.tail) > 8192 || cap(buffer.tail) > 8192 {
				t.Fatal("stderr capture did not retain the bounded exact tail")
			}
		}
	})
	t.Run("HTTP rejection metadata", func(t *testing.T) {
		harness := &ephemeralIntegrationHarness{}
		for _, test := range []struct {
			method, path string
			body         io.Reader
			status       int
		}{
			{http.MethodGet, "/v1/responses", nil, http.StatusNotFound},
			{http.MethodPost, "/unexpected", nil, http.StatusNotFound},
			{http.MethodPost, "/v1/responses", strings.NewReader("invalid JSON"), http.StatusBadRequest},
			{http.MethodPost, "/v1/responses", iotest.ErrReader(io.ErrUnexpectedEOF), http.StatusBadRequest},
		} {
			request := httptest.NewRequest(test.method, test.path, test.body)
			request.Header.Set("Content-Encoding", "synthetic-encoding")
			response := httptest.NewRecorder()
			harness.respond(response, request)
			if response.Code != test.status {
				t.Fatalf("HTTP rejection=%d, want %d", response.Code, test.status)
			}
		}
		if len(harness.httpRequests) != 4 || len(harness.requests) != 0 || harness.httpRequests[0].Method != http.MethodGet ||
			harness.httpRequests[1].Path != "/unexpected" || harness.httpRequests[2].ContentEncoding != "synthetic-encoding" ||
			harness.httpRequests[2].Error == "" || harness.httpRequests[3].Error != io.ErrUnexpectedEOF.Error() {
			t.Fatal("rejected HTTP traffic was lost or admitted as Responses evidence")
		}
		for index := 0; index < 70; index++ {
			request := httptest.NewRequest(http.MethodGet, "/"+strings.Repeat("p", 1024), nil)
			request.Header.Set("Content-Encoding", strings.Repeat("e", 256))
			harness.respond(httptest.NewRecorder(), request)
		}
		if len(harness.httpRequests) != 64 || len(harness.httpRequests[4].Path) > 512 || len(harness.httpRequests[4].ContentEncoding) > 128 || len(harness.requests) != 0 {
			t.Fatal("HTTP diagnostic bounds or separate Responses eligibility changed")
		}
	})
}

// Diagnostics retain only fixed categories and synthetic owned identities. RPC
// parameters, error data/messages, model requests and file bytes are never kept.
type ephemeralIntegrationPhase struct {
	Phase                           string
	Direction                       string
	Method                          string
	ErrorCode                       int
	Summary                         string
	ThreadID, TurnID                string
	ProviderPOSTs, ProviderUpgrades int
	DeniedPackets                   uint64
	CounterUnavailable              bool
	CounterPending, CounterSampled  bool
	ObservedMicros, CounterMicros   int64
}

func integrationRPCSummary(failure *rpcError) string {
	if failure == nil {
		return "ok"
	}
	// Inspect only a bounded prefix; never echo native diagnostics or paths.
	message := integrationDiagnosticText(failure.Message, 4096)
	if strings.Contains(message, "experimental compact prompt file is empty:") {
		return "empty-compact-instruction-file"
	}
	if strings.Contains(message, "model instructions file is empty:") {
		return "empty-model-instruction-file"
	}
	return "other-native-error"
}

func integrationRPCPhase(method string) (string, string) {
	switch method {
	case "initialize", "initialized":
		return "handshake", method
	case "config/read", "configRequirements/read":
		return "configuration", method
	case "model/list":
		return "model-lookup", method
	case "thread/start":
		return "thread-start", method
	case "turn/start", "turn/started":
		return "turn-start", method
	case "turn/interrupt", "thread/unsubscribe":
		return "teardown", method
	case "item/tool/requestUserInput", "item/tool/call", "item/commandExecution/requestApproval":
		return "tool-dispatch", method
	case "item/started", "item/completed", "turn/completed", "error":
		return "turn-event", method
	default:
		return "other", "unrecognized"
	}
}

func (h *ephemeralIntegrationHarness) capturePhase(generation uint64, phase, direction, method string, failure *rpcError, threadID, turnID string) {
	h.mu.Lock()
	final := phase == "after-utility-teardown"
	if generation != h.diagnosticGeneration || !h.diagnosticsActive || h.diagnosticFinal || (!final && len(h.phases) >= 47) {
		h.mu.Unlock()
		return
	}
	if ephemeralID(threadID) {
		h.diagnosticThread = threadID
	}
	if ephemeralID(turnID) && threadID == h.diagnosticThread {
		h.diagnosticTurn = turnID
	}
	event := ephemeralIntegrationPhase{Phase: phase, Direction: direction, Method: method, Summary: integrationRPCSummary(failure),
		ThreadID: h.diagnosticThread, TurnID: h.diagnosticTurn, ProviderPOSTs: len(h.requests), ProviderUpgrades: h.utilityUpgrades,
		ObservedMicros: time.Since(h.diagnosticStarted).Microseconds()}
	if failure != nil {
		event.ErrorCode = failure.Code
	}
	boundary := final || phase == "before-utility"
	sample := boundary || !h.diagnosticReadActive
	if sample {
		event.CounterPending = true
		if !boundary {
			h.diagnosticReadActive = true
		}
	}
	index := len(h.phases)
	h.phases = append(h.phases, event)
	if final {
		h.diagnosticFinal = true
	}
	reader := h.diagnosticCounter
	if reader == nil {
		reader = h.readDroppedPackets
	}
	h.mu.Unlock()
	if !sample {
		return
	}
	read := func() {
		// Only one counter read can be in flight. Normal proxy captures never
		// wait for nft; boundary reads happen outside the helper's fixed budget.
		h.diagnosticMu.Lock()
		defer h.diagnosticMu.Unlock()
		h.mu.Lock()
		if generation != h.diagnosticGeneration || !h.diagnosticsActive {
			h.mu.Unlock()
			return
		}
		if !boundary && h.diagnosticFinal {
			h.phases[index].CounterPending = false
			h.diagnosticReadActive = false
			h.mu.Unlock()
			return
		}
		h.diagnosticReads++
		h.mu.Unlock()
		packets, err := reader()
		h.mu.Lock()
		if generation == h.diagnosticGeneration && h.diagnosticsActive {
			stored := &h.phases[index]
			stored.DeniedPackets, stored.CounterUnavailable = packets, err != nil
			stored.CounterPending, stored.CounterSampled = false, true
			stored.CounterMicros = time.Since(h.diagnosticStarted).Microseconds()
			if !boundary {
				h.diagnosticReadActive = false
			}
			if final {
				// In-flight reads completed before this boundary took diagnosticMu.
				// Queued reads are now fenced out and must not run after the final.
				for i := range h.phases {
					h.phases[i].CounterPending = false
				}
				h.diagnosticReadActive = false
			}
		}
		h.mu.Unlock()
	}
	if boundary {
		read()
	} else {
		go read()
	}
}

func (h *ephemeralIntegrationHarness) captureRPC(generation uint64, direction, method string, failure *rpcError, threadID, turnID string) {
	phase, safeMethod := integrationRPCPhase(method)
	if phase != "other" || failure != nil {
		h.capturePhase(generation, phase, direction, safeMethod, failure, threadID, turnID)
	}
}

func (h *ephemeralIntegrationHarness) beginDiagnostics() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.diagnosticGeneration++
	h.diagnosticStarted = time.Now()
	h.phases = nil
	h.diagnosticThread, h.diagnosticTurn = "", ""
	h.diagnosticReads = 0
	h.diagnosticReadActive, h.diagnosticFinal = false, false
	h.diagnosticsActive = true
	h.diagnosticCallActive = false
	h.diagnosticCallContext = nil
	h.withheldResponse = ""
	h.toolDiagnostic = ""
	h.questionDispatched, h.questionObserved = false, false
	return h.diagnosticGeneration
}

func (h *ephemeralIntegrationHarness) noteWithheldResponse(generation uint64, method string, message rpcMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if generation == h.diagnosticGeneration && h.diagnosticCallActive && h.diagnosticCallContext != nil &&
		h.diagnosticCallContext.Err() == nil && message.Method == "" && message.Error == nil && len(message.Result) > 0 {
		h.withheldResponse = method
	}
}

func (h *ephemeralIntegrationHarness) timeoutFaultExercised(generation uint64, mode string) bool {
	want := ""
	switch mode {
	case "timeout-config":
		want = "config/read"
	case "timeout-model":
		want = "model/list"
	default:
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return generation == h.diagnosticGeneration && h.withheldResponse == want
}

func TestEphemeralIntegrationRPCDiagnostics(t *testing.T) {
	for _, test := range []struct{ name, message, want string }{
		{"model file", "failed: model instructions file is empty: /private/SECRET", "empty-model-instruction-file"},
		{"compact file", "experimental compact prompt file is empty: /private/SECRET", "empty-compact-instruction-file"},
		{"other", "SECRET arbitrary native diagnostic", "other-native-error"},
		{"bounded", strings.Repeat("x", 4096) + "model instructions file is empty: SECRET", "other-native-error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := &rpcError{Code: -32600, Message: test.message, Data: json.RawMessage(`{"secret":"never retain"}`)}
			if got := integrationRPCSummary(failure); got != test.want || strings.Contains(got, "SECRET") {
				t.Fatalf("unsafe category=%q", got)
			}
		})
	}
	if phase, method := integrationRPCPhase("arbitrary-SECRET-method"); phase != "other" || method != "unrecognized" {
		t.Fatal("unknown RPC method escaped fixed categories")
	}
	// Count actual reads as well as stored entries, without a native subprocess.
	reads := 0
	h := &ephemeralIntegrationHarness{diagnosticCounter: func() (uint64, error) { reads++; return 0, errors.New("SECRET") }}
	generation := h.beginDiagnostics()
	for index := 0; index < 60; index++ {
		h.capturePhase(generation, "before-utility", "boundary", "none", &rpcError{Code: -32600, Message: "SECRET"}, "synthetic-thread", "")
	}
	if len(h.phases) != 47 || reads != 47 || h.diagnosticReads != 47 || h.phases[0].ErrorCode != -32600 || !h.phases[0].CounterUnavailable || h.phases[0].ThreadID != "synthetic-thread" {
		t.Fatal("bounded setup evidence lost")
	}
	h.capturePhase(generation, "after-utility-teardown", "boundary", "none", nil, "", "")
	h.capturePhase(generation, "after-utility-teardown", "boundary", "none", nil, "", "")
	if len(h.phases) != 48 || reads != 48 || h.diagnosticReads != 48 || h.phases[47].Phase != "after-utility-teardown" || !h.phases[47].CounterSampled {
		t.Fatal("full diagnostics lost the final unchanged counter sample")
	}
	encoded, _ := json.Marshal(h.phases)
	if bytes.Contains(encoded, []byte("SECRET")) || bytes.Contains(encoded, []byte("never retain")) {
		t.Fatal("native error text/data retained")
	}
}

func TestEphemeralIntegrationDiagnosticOwnership(t *testing.T) {
	t.Run("delayed old capture", func(t *testing.T) {
		var reads atomic.Int32
		h := &ephemeralIntegrationHarness{diagnosticCounter: func() (uint64, error) { reads.Add(1); return 17, nil }}
		old := h.beginDiagnostics()
		release, done := make(chan struct{}), make(chan struct{})
		go func() {
			<-release
			h.captureRPC(old, "response", "thread/start", nil, "old-thread", "old-turn")
			close(done)
		}()
		current := h.beginDiagnostics()
		close(release)
		<-done
		if len(h.phases) != 0 || reads.Load() != 0 || h.diagnosticThread != "" || h.diagnosticTurn != "" {
			t.Fatal("delayed old proxy contaminated the next leaf")
		}
		h.capturePhase(current, "before-utility", "boundary", "none", nil, "new-thread", "new-turn")
		if len(h.phases) != 1 || reads.Load() != 1 || h.phases[0].ThreadID != "new-thread" || h.phases[0].TurnID != "new-turn" {
			t.Fatal("current leaf lost its own diagnostic evidence")
		}
	})
	t.Run("old counter completion", func(t *testing.T) {
		var reads atomic.Int32
		started, release := make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		h := &ephemeralIntegrationHarness{diagnosticCounter: func() (uint64, error) {
			if reads.Add(1) == 1 {
				close(started)
				<-release
				return 99, nil
			}
			return 17, nil
		}}
		old := h.beginDiagnostics()
		h.captureRPC(old, "response", "thread/start", nil, "old-thread", "old-turn")
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("controlled counter did not start")
		}
		current := h.beginDiagnostics()
		close(release)
		h.capturePhase(current, "before-utility", "boundary", "none", nil, "new-thread", "new-turn")
		if len(h.phases) != 1 || h.diagnosticReads != 1 || reads.Load() != 2 || h.phases[0].DeniedPackets != 17 || h.phases[0].ThreadID != "new-thread" {
			t.Fatal("old counter result or invocation was attributed to the next leaf")
		}
	})
}

func TestEphemeralIntegrationTimeoutReachability(t *testing.T) {
	for _, mode := range []string{"timeout-config", "timeout-model"} {
		t.Run(mode+" with slow counter", func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer close(release)
			h := &ephemeralIntegrationHarness{utilitySocket: (&ephemeralFake{}).serve(t), diagnosticCounter: func() (uint64, error) {
				once.Do(func() { close(started) })
				<-release
				return 0, nil
			}}
			generation := h.beginDiagnostics()
			client := New(h.proxy(t, mode, generation))
			defer client.Close()
			opts := ephemeralTestOptions(t)
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			h.mu.Lock()
			h.diagnosticCallContext, h.diagnosticCallActive = ctx, true
			h.mu.Unlock()
			_, err := client.RunEphemeralTurn(ctx, opts)
			h.mu.Lock()
			h.diagnosticCallActive = false
			h.mu.Unlock()
			if !errors.Is(err, context.DeadlineExceeded) || !h.timeoutFaultExercised(generation, mode) {
				t.Fatalf("slow diagnostic obscured intended withheld reply: mode=%s error=%v", mode, err)
			}
			select {
			case <-started:
			default:
				t.Fatal("controlled slow counter was not exercised")
			}
			h.mu.Lock()
			blocked := h.diagnosticReadActive && h.diagnosticReads == 1
			h.diagnosticsActive = false
			h.mu.Unlock()
			if !blocked {
				t.Fatal("slow counter was not held while the intended reply was reached")
			}
		})
		t.Run(mode+" earlier handshake timeout", func(t *testing.T) {
			release := make(chan struct{})
			handshakeStarted := make(chan struct{}, 1)
			defer close(release)
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				defer connection.CloseNow()
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				if request["method"] != "initialize" {
					return errors.New("negative control expected initialize request")
				}
				select {
				case handshakeStarted <- struct{}{}:
				default:
				}
				<-release // No initialize response; the target reply cannot be reached.
				return nil
			})
			h := &ephemeralIntegrationHarness{utilitySocket: socket, diagnosticCounter: func() (uint64, error) { return 0, nil }}
			generation := h.beginDiagnostics()
			client := New(h.proxy(t, mode, generation))
			defer client.Close()
			opts := ephemeralTestOptions(t)
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			h.mu.Lock()
			h.diagnosticCallContext, h.diagnosticCallActive = ctx, true
			h.mu.Unlock()
			_, err := client.RunEphemeralTurn(ctx, opts)
			h.mu.Lock()
			h.diagnosticCallActive = false
			h.mu.Unlock()
			// Handshake teardown may report a static server error at inference
			// expiry. Neither error category establishes target-response coverage.
			if err == nil || h.timeoutFaultExercised(generation, mode) {
				t.Fatalf("failed early handshake counted as target coverage: mode=%s error=%v", mode, err)
			}
			select {
			case <-handshakeStarted:
			default:
				t.Fatal("negative control never reached the withheld initialize request")
			}
		})
	}
	for _, scenario := range []string{"stopped", "expired", "foreign generation", "native error", "server request", "other method"} {
		t.Run(scenario+" reply is not coverage", func(t *testing.T) {
			h := &ephemeralIntegrationHarness{}
			generation := h.beginDiagnostics()
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			h.diagnosticCallContext, h.diagnosticCallActive = ctx, true
			method := "config/read"
			message := rpcMessage{Result: json.RawMessage(`{}`)}
			switch scenario {
			case "stopped":
				h.diagnosticCallActive = false
			case "expired":
				cancel()
			case "foreign generation":
				generation++
			case "native error":
				message.Error = &rpcError{Code: -32600, Message: "SECRET"}
			case "server request":
				message.Method = "config/read"
			case "other method":
				method = "model/list"
			}
			h.noteWithheldResponse(generation, method, message)
			if h.timeoutFaultExercised(h.diagnosticGeneration, "timeout-config") {
				t.Fatal("unowned or inactive reply counted as coverage")
			}
		})
	}
}

type ephemeralIntegrationHarness struct {
	t                                                             *testing.T
	binary, nft, home, project, socket, canary, canaryURL, config string
	provider                                                      *httptest.Server
	canaryServer                                                  *httptest.Server
	canaryHits                                                    atomic.Int32
	utilityCommand                                                *exec.Cmd
	utilityStopped                                                *ephemeralIntegrationWait
	utilitySocket, catalog, alternateCatalog, startupCwd          string
	command                                                       *exec.Cmd
	stderr                                                        *ephemeralIntegrationStderr
	stopped                                                       *ephemeralIntegrationWait
	namespaceOwner                                                ephemeralIntegrationNamespaceOwner
	mu                                                            sync.Mutex
	diagnosticMu                                                  sync.Mutex
	diagnosticGeneration                                          uint64
	diagnosticStarted                                             time.Time
	diagnosticReads                                               int
	diagnosticReadActive, diagnosticFinal                         bool
	diagnosticCounter                                             func() (uint64, error)
	diagnosticCallContext                                         context.Context
	diagnosticCallActive                                          bool
	withheldResponse                                              string
	diagnosticsActive                                             bool
	phases                                                        []ephemeralIntegrationPhase
	diagnosticThread, diagnosticTurn                              string
	mode, namespace, attack                                       string
	requests                                                      []map[string]any
	httpRequests                                                  []*ephemeralIntegrationHTTPRequest
	controlUpgrades, utilityUpgrades                              int
	failure                                                       string
	toolDiagnostic                                                string
	questionDispatched, questionObserved                          bool
	controlMode                                                   bool
	proxyError                                                    atomic.Bool
	threadIDs                                                     []string
	baseline                                                      map[string]string
	ordinary                                                      *Client
	controlThread                                                 string
	controlTurn                                                   string
	egressDrops                                                   uint64
	activeCancel                                                  context.CancelFunc
	heldContinuation                                              chan struct{}
	continuationStopped                                           chan struct{}
	interrupted                                                   bool
	catalogConfirmed                                              bool
	refreshDelivered                                              bool
}

func newEphemeralIntegrationHarness(t *testing.T, binary, nft string, owner ephemeralIntegrationNamespaceOwner) *ephemeralIntegrationHarness {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "epi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	h := &ephemeralIntegrationHarness{t: t, namespaceOwner: owner, binary: binary, nft: nft, home: filepath.Join(root, "home"), project: filepath.Join(root, "project"), socket: filepath.Join(root, "codex.sock"), canary: filepath.Join(root, "canary"), mode: "baseline"}
	for _, path := range []string{h.home, h.project} {
		if os.Mkdir(path, 0700) != nil {
			t.Fatal("private fixture state unavailable")
		}
	}
	h.write(t, h.canary, "ACTUAL_FILE_READ_CANARY_8cdbe5", 0600)
	h.canaryServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.canaryHits.Add(1); w.WriteHeader(500) }))
	h.canaryURL = h.canaryServer.URL
	h.provider = httptest.NewServer(http.HandlerFunc(h.respond))
	providerURL, _ := url.Parse(h.provider.URL)
	rules := fmt.Sprintf(`table inet ephemeral_fixture {
	chain output {
		type filter hook output priority 0; policy drop;
		oifname lo ip daddr 127.0.0.1 tcp dport %s accept;
		oifname lo ip saddr 127.0.0.1 tcp sport %s accept;
		counter drop;
	}
}
`, providerURL.Port(), providerURL.Port())
	firewall := exec.Command(nft, "-f", "-")
	firewall.Stdin = strings.NewReader(rules)
	if output, err := firewall.CombinedOutput(); err != nil {
		t.Fatalf("egress guard unavailable: %v %s", err, output)
	}
	probe := &http.Client{Timeout: 100 * time.Millisecond}
	response, err := probe.Get(h.provider.URL + "/v1/responses")
	if err != nil {
		t.Fatalf("egress guard blocked the provider endpoint: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("provider connectivity probe returned %d, expected 404", response.StatusCode)
	}
	beforeProbe := h.droppedPackets(t)
	canaryURL, _ := url.Parse(h.canaryURL)
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	blocked, probeErr := integrationBlockedProbe(probeCtx, canaryURL.Host)
	probeCancel()
	if probeErr != nil {
		t.Fatal(probeErr)
	}
	if !blocked {
		t.Fatal("egress guard allowed canary endpoint")
	}
	if h.droppedPackets(t) <= beforeProbe {
		t.Fatal("owned blocked probe produced no denied packets")
	}
	if h.canaryHits.Load() != 0 {
		t.Fatal("egress guard canary was reached")
	}
	h.catalog, err = filepath.EvalSymlinks(filepath.Join(os.Getenv("CODEX_EPHEMERAL_TEST_SOURCE"), "models-manager/models.json"))
	if err != nil || !validEphemeralCatalog(h.catalog) {
		t.Fatal("immutable original catalog unavailable")
	}
	h.utilitySocket = filepath.Join(root, "naming.sock")
	h.startupCwd = filepath.Join(root, "startup")
	if err := os.Mkdir(h.startupCwd, 0700); err != nil {
		t.Fatal("private naming startup cwd unavailable")
	}
	h.alternateCatalog = filepath.Join(h.home, "original-models.json")
	catalogBytes, err := os.ReadFile(h.catalog)
	if err != nil || os.WriteFile(h.alternateCatalog, catalogBytes, 0444) != nil {
		t.Fatal("full original alternate path unavailable")
	}
	h.seed(t, "literal.name")
	h.start(t)
	return h
}

func integrationBlockedProbe(ctx context.Context, address string) (bool, error) {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	number, portErr := strconv.Atoi(port)
	if err != nil || ip == nil || ip.To4() == nil || ip.String() != host || !ip.IsLoopback() || portErr != nil || number < 1 || number > 65535 {
		return false, errors.New("invalid synthetic IPv4 probe endpoint")
	}
	// The single serial dial owns cancellation and has finished before return.
	// No shared HTTP transport retains a detached blocked connect attempt.
	connection, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	if connection != nil {
		connection.Close()
	}
	return dialErr != nil, nil
}

func TestEphemeralIntegrationOwnedProbe(t *testing.T) {
	t.Run("permitted connection is refused and closed by the probe", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		closed := make(chan error, 1)
		go func() {
			connection, err := listener.Accept()
			if err != nil {
				closed <- err
				return
			}
			defer connection.Close()
			connection.SetReadDeadline(time.Now().Add(time.Second))
			var buffer [1]byte
			_, err = connection.Read(buffer[:])
			closed <- err
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		blocked, err := integrationBlockedProbe(ctx, listener.Addr().String())
		if err != nil || blocked {
			t.Fatal("a permitted endpoint was reported blocked")
		}
		select {
		case err := <-closed:
			if !errors.Is(err, io.EOF) {
				t.Fatal("owned probe left its connection open")
			}
		case <-time.After(time.Second):
			t.Fatal("owned probe did not finish")
		}
	})
	t.Run("cancellation finishes without a delayed connection", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		blocked, err := integrationBlockedProbe(ctx, listener.Addr().String())
		if err != nil || !blocked {
			t.Fatal("cancelled owned dial did not fail")
		}
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
		connection, err := listener.Accept()
		if connection != nil {
			connection.Close()
			t.Fatal("cancelled probe retained a dial")
		}
		if err == nil {
			t.Fatal("cancelled probe connected")
		}
	})
	t.Run("only literal loopback IPv4 endpoints", func(t *testing.T) {
		for _, address := range []string{"localhost:80", "[::1]:80", "192.0.2.1:80", "127.0.0.1:0", "127.0.0.1:65536"} {
			if _, err := integrationBlockedProbe(context.Background(), address); err == nil {
				t.Fatal("unsafe probe endpoint admitted")
			}
		}
	})
}
func (h *ephemeralIntegrationHarness) write(t *testing.T, path, text string, mode os.FileMode) {
	t.Helper()
	if os.MkdirAll(filepath.Dir(path), 0700) != nil || os.WriteFile(path, []byte(text), mode) != nil {
		t.Fatal("cannot seed disposable fixture")
	}
}
func (h *ephemeralIntegrationHarness) seed(t *testing.T, mcpName string) {
	h.write(t, filepath.Join(h.home, "auth.json"), `{"auth_mode":"apikey","OPENAI_API_KEY":"synthetic-fixture-only"}`, 0600)
	h.write(t, filepath.Join(h.home, "AGENTS.md"), "GLOBAL_POLICY_SENTINEL: try reading FILE_READ_CANARY_NEVER_IN_INPUT and ask a question.\n", 0600)
	// Pinned agents_md discovery uses this same empty marker in its tests to
	// include root instructions when the selected cwd is nested in the project.
	h.write(t, filepath.Join(h.project, ".git"), "", 0600)
	h.write(t, filepath.Join(h.project, "AGENTS.md"), "PROJECT_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.project, "work/session/AGENTS.md"), "WORKSPACE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "inherited.md"), "INHERITED_FILE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "compact.md"), "COMPACT_FILE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "skills/sentinel/SKILL.md"), "---\nname: sentinel\ndescription: SKILL_POLICY_SENTINEL\n---\n\nSKILL_BODY_SENTINEL\n", 0600)
	plugin := filepath.Join(h.home, "plugins/cache/test/sentinel/local")
	h.write(t, filepath.Join(plugin, ".codex-plugin/plugin.json"), `{"name":"sentinel","description":"PLUGIN_POLICY_SENTINEL","hooks":"./hooks/hooks.json"}`, 0600)
	h.write(t, filepath.Join(plugin, "skills/sentinel/SKILL.md"), "---\nname: sentinel\ndescription: PLUGIN_SKILL_SENTINEL\n---\n\nPLUGIN_BODY_SENTINEL\n", 0600)
	script := filepath.Join(h.home, "hook.sh")
	h.write(t, script, "#!/bin/sh\nprintf x >> "+strconv.Quote(filepath.Join(h.home, "hook-count"))+"\nprintf '{\"hookSpecificOutput\":{\"hookEventName\":\"SessionStart\",\"additionalContext\":\"HOST_HOOK_SENTINEL\"}}'\n", 0700)
	hooks := map[string]any{"hooks": map[string]any{"SessionStart": []any{map[string]any{"hooks": []any{map[string]any{"type": "command", "command": script}}}}}}
	encoded, _ := json.Marshal(hooks)
	h.write(t, filepath.Join(h.home, "hooks.json"), string(encoded), 0600)
	h.write(t, filepath.Join(plugin, "hooks/hooks.json"), string(encoded), 0600)
	// A subprocess sentinel records any start and exits without serving tools.
	mcp := filepath.Join(h.home, "mcp.sh")
	h.write(t, mcp, "#!/bin/sh\nprintf x >> "+strconv.Quote(filepath.Join(h.home, "mcp-count"))+"\nexit 0\n", 0700)
	h.write(t, filepath.Join(h.home, "memories/MEMORY.md"), "MEMORY_POLICY_SENTINEL\n", 0600)
	h.config = fmt.Sprintf("model = \"gpt-6-luna\"\nmodel_reasoning_effort = \"low\"\nmodel_provider = \"openai\"\nopenai_base_url = %s\nmodel_instructions_file = %s\nexperimental_compact_prompt_file = %s\nnotify = [%s]\n[features]\nshell_tool = true\nunified_exec = true\nview_image = true\nimage_generation = true\nstandalone_web_search = true\napps = true\nplugins = true\nhooks = true\nmemories = true\ngoals = true\nmulti_agent = true\nskill_search = true\nenable_request_compression = false\n[plugins.\"sentinel@test\"]\nenabled = true\n[mcp_servers.%s]\ncommand = %s\nstartup_timeout_sec = 0.2\n[mcp_servers.http_sentinel]\nurl = %s\nstartup_timeout_sec = 0.2\n", strconv.Quote(h.provider.URL+"/v1"), strconv.Quote(filepath.Join(h.home, "inherited.md")), strconv.Quote(filepath.Join(h.home, "compact.md")), strconv.Quote(script), strconv.Quote(mcpName), strconv.Quote(mcp), strconv.Quote(h.canaryURL))
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
}
func (h *ephemeralIntegrationHarness) start(t *testing.T) {
	if h.namespaceOwner.recheck(integrationNamespaceObservation()) != nil {
		t.Fatal("native startup requires proven private PID namespace")
	}
	if h.command != nil || h.utilityCommand != nil {
		t.Fatal("native startup preceded positive stop")
	}
	h.stderr = &ephemeralIntegrationStderr{}
	h.mu.Lock()
	h.httpRequests = nil
	h.controlUpgrades, h.utilityUpgrades = 0, 0
	h.mu.Unlock()
	// Only synthetic credentials; no HOME/config/token environment is inherited.
	h.command = exec.Command(h.binary, "app-server", "--listen", "unix://"+h.socket)
	h.command.Dir = h.project
	h.command.Env = []string{"HOME=" + h.home, "CODEX_HOME=" + h.home, "PATH=" + os.Getenv("PATH")}
	h.command.Stdout = io.Discard
	h.command.Stderr = h.stderr
	if err := h.command.Start(); err != nil {
		t.Fatal("candidate app-server launch failed")
	}
	h.stopped = integrationCommandWait(h.command)
	h.utilityCommand = exec.Command(h.binary, "-c", "model_catalog_json="+strconv.Quote(h.catalog), "app-server", "--strict-config", "--listen", "unix://"+h.utilitySocket)
	h.utilityCommand.Dir = h.startupCwd
	h.utilityCommand.Env = []string{"HOME=" + h.home, "CODEX_HOME=" + h.home, "PATH=" + os.Getenv("PATH"), "CODEX_INTERNAL_APP_SERVER_REMOTE_CONTROL_DISABLED=1"}
	h.utilityCommand.Stdout = io.Discard
	h.utilityCommand.Stderr = h.stderr
	if err := h.utilityCommand.Start(); err != nil {
		t.Fatal("static naming candidate launch failed")
	}
	h.utilityStopped = integrationCommandWait(h.utilityCommand)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(h.socket); err == nil {
			if _, utilityErr := os.Stat(h.utilitySocket); utilityErr == nil {
				return
			}
		}
		if done, _ := h.stopped.result(); done {
			t.Fatal("candidate exited before socket readiness")
		}
		if done, _ := h.utilityStopped.result(); done {
			t.Fatal("static candidate exited before socket readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("candidate socket readiness exceeded fixture budget")
}
func (h *ephemeralIntegrationHarness) stop() {
	h.mu.Lock()
	if h.diagnosticCallActive {
		h.mu.Unlock()
		h.t.Fatal("native stop overlapped active utility call")
	}
	h.diagnosticGeneration++
	h.diagnosticsActive = false
	h.mu.Unlock()
	// Drain the current bounded nft read, then exclude queued reads until both
	// Cmd.Wait owners finish and the namespace returns ECHILD. No subprocess
	// helper, restart, snapshot or seed may run in this interval.
	h.diagnosticMu.Lock()
	err := integrationStopNamespace(h.namespaceOwner, [2]*ephemeralIntegrationWait{h.stopped, h.utilityStopped}, integrationNamespaceStopOps())
	h.diagnosticMu.Unlock()
	if err != nil {
		h.t.Fatal(err)
	}
	h.command, h.utilityCommand = nil, nil
	h.stopped, h.utilityStopped = nil, nil
	_ = os.Remove(h.utilitySocket)
	_ = os.Remove(h.socket)
}
func (h *ephemeralIntegrationHarness) close() {
	if h.ordinary != nil {
		h.ordinary.Close()
	}
	h.stop()
	if h.provider != nil {
		h.provider.Close()
	}
	if h.canaryServer != nil {
		h.canaryServer.Close()
	}
}

func (h *ephemeralIntegrationHarness) respond(w http.ResponseWriter, r *http.Request) {
	// Retain this handler's origin even if parsing/delivery resumes in a later
	// leaf. A late callback must not acquire the next leaf's diagnostic token.
	h.mu.Lock()
	generation := h.diagnosticGeneration
	h.mu.Unlock()
	diagnostic := &ephemeralIntegrationHTTPRequest{Method: integrationDiagnosticText(r.Method, 32),
		Path: integrationDiagnosticText(r.URL.Path, 512), ContentEncoding: integrationDiagnosticText(r.Header.Get("Content-Encoding"), 128),
		Connection: integrationDiagnosticText(r.Header.Get("Connection"), 128), Upgrade: integrationDiagnosticText(r.Header.Get("Upgrade"), 128),
		WebSocketVersion: integrationDiagnosticText(r.Header.Get("Sec-WebSocket-Version"), 32), UpgradeAccepted: integrationWebSocketUpgrade(r)}
	h.mu.Lock()
	if len(h.httpRequests) < 64 {
		h.httpRequests = append(h.httpRequests, diagnostic)
	}
	if diagnostic.UpgradeAccepted {
		if h.controlMode {
			h.controlUpgrades = min(h.controlUpgrades+1, 64)
		} else {
			h.utilityUpgrades = min(h.utilityUpgrades+1, 64)
		}
	}
	h.mu.Unlock()
	if diagnostic.UpgradeAccepted {
		h.capturePhase(generation, "provider-handshake", "request", "accepted-WS-upgrade", nil, "", "")
		// The native builtin provider selects HTTP fallback after this supported
		// refusal. Only decoded POSTs below establish inference eligibility.
		w.WriteHeader(http.StatusUpgradeRequired)
		return
	}
	if r.Method != "POST" || r.URL.Path != "/v1/responses" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 256*1024+1))
	if err != nil || len(raw) > 256*1024 {
		if err == nil {
			err = errors.New("provider request body bound exceeded")
		}
		h.mu.Lock()
		diagnostic.Error = integrationDiagnosticText(err.Error(), 512)
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		h.mu.Lock()
		diagnostic.Error = integrationDiagnosticText(err.Error(), 512)
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	h.mu.Lock()
	if len(h.requests) >= 64 {
		h.failure = "provider request bound exceeded"
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	h.requests = append(h.requests, request)
	index := len(h.requests)
	mode, namespace, attack, control := h.mode, h.namespace, h.attack, h.controlMode
	if !control {
		if request["model"] != "gpt-5.5" {
			h.failure = "model substitution"
		}
		reasoning, _ := request["reasoning"].(map[string]any)
		if reasoning["effort"] != "low" {
			h.failure = "effort substitution"
		}
		questionControl := mode == "server-question-control" || mode == "question-at-cancel"
		names, toolErr := integrationToolNames(request, questionControl)
		expectedCount := 0
		if questionControl {
			expectedCount = 1
		}
		if toolErr != nil || len(names) != expectedCount || (questionControl && !names["request_user_input"]) {
			h.failure = "unexpected model-facing tool schemas"
			if generation == h.diagnosticGeneration && h.toolDiagnostic == "" {
				h.toolDiagnostic = integrationToolDiagnostic(request, names, toolErr, expectedCount)
			}
		}
		if instructions, ok := request["instructions"].(string); !ok || !strings.Contains(instructions, "Return the requested JSON.") {
			h.failure = "explicit base instructions missing from Direct request"
		}
		if request["store"] != false || request["stream"] != true {
			h.failure = "unexpected Direct persistence or streaming request"
		}
		text := string(raw)
		if !strings.Contains(text, "GLOBAL_POLICY_SENTINEL") {
			h.failure = "trusted global policy missing"
		}
		for _, sentinel := range []string{"PROJECT_POLICY_SENTINEL", "WORKSPACE_POLICY_SENTINEL", "TEAM_POLICY_SENTINEL", "SKILL_POLICY_SENTINEL", "PLUGIN_POLICY_SENTINEL", "PLUGIN_SKILL_SENTINEL", "INHERITED_FILE_POLICY_SENTINEL", "COMPACT_FILE_POLICY_SENTINEL", "HOST_HOOK_SENTINEL", "MEMORY_POLICY_SENTINEL", "GOAL_STATE_SENTINEL", "FILE_READ_CANARY_NEVER_IN_INPUT"} {
			// The global instruction names the file-read marker without its content;
			// the actual secret below is checked separately in run().
			if sentinel == "FILE_READ_CANARY_NEVER_IN_INPUT" {
				continue
			}
			if strings.Contains(text, sentinel) {
				h.failure = "excluded instruction source present"
			}
		}
	}
	held, stopped := h.heldContinuation, h.continuationStopped
	h.mu.Unlock()
	if !control {
		h.capturePhase(generation, "provider-inference", "request", "Responses-POST", nil, "", "")
	}
	if mode == "lost-turn" {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"held-response\"}}\n\n")
		if flush, ok := w.(http.Flusher); ok {
			flush.Flush()
		}
		close(held)
		// Live tool-free inference can end only when the exact owned turn is
		// interrupted; no natural provider completion hides a lost-reply leak.
		<-r.Context().Done()
		close(stopped)
		return
	}
	if mode == "timeout" {
		<-r.Context().Done()
		return
	}
	if mode == "static-refresh" {
		w.Header().Set("x-models-etag", "synthetic-static-refresh")
		h.mu.Lock()
		if generation == h.diagnosticGeneration {
			h.refreshDelivered = true
		}
		h.mu.Unlock()
	}
	w.Header().Set("Content-Type", "text/event-stream")
	event := func(value any) { data, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", data) }
	event(map[string]any{"type": "response.created", "response": map[string]any{"id": fmt.Sprintf("response-%d", index)}})
	final := func() {
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": fmt.Sprintf("answer-%d", index), "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": `{"name":"fix runtime session names"}`}}}})
	}
	call := func(name, ns, args string, custom bool) {
		if mode == "question-at-cancel" && name == "request_user_input" {
			h.mu.Lock()
			if generation == h.diagnosticGeneration {
				h.questionDispatched = true
			}
			h.mu.Unlock()
		}
		if !control {
			h.capturePhase(generation, "tool-dispatch", "provider-output", "synthetic-tool-call", nil, "", "")
		}
		item := map[string]any{"type": "function_call", "name": name, "call_id": fmt.Sprintf("call-%d", index), "arguments": args}
		if ns != "" {
			item["namespace"] = ns
		}
		if custom {
			item["type"] = "custom_tool_call"
			delete(item, "arguments")
			item["input"] = args
		}
		event(map[string]any{"type": "response.output_item.done", "item": item})
	}
	switch {
	case control:
		final()
	case (mode == "server-question-control" || mode == "question-at-cancel") && index == 1:
		call("request_user_input", "", `{"questions":[{"id":"q","header":"Question","question":"Synthetic question?","options":[{"label":"Continue","description":"Continue the fixture."},{"label":"Stop","description":"Stop the fixture."}]}]}`, false)
	case mode == "forbidden" && index == 1:
		parts := strings.SplitN(attack, "\n", 2)
		args := "{}"
		if len(parts) == 2 {
			args = parts[1]
		}
		args = strings.ReplaceAll(args, "CANARY_URL", h.canaryURL)
		args = strings.ReplaceAll(args, "CANARY", h.canary)
		call(parts[0], namespace, args, parts[0] == "apply_patch" || parts[0] == "exec")
	default:
		final()
	}
	event(map[string]any{"type": "response.completed", "response": map[string]any{"id": fmt.Sprintf("response-%d", index), "usage": map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}})
}

// These private summaries never retain tool descriptions, schemas or input text.
func integrationDiagnosticIdentifier(value any) string {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > 64 {
		return "[invalid]"
	}
	for i, c := range []byte(text) {
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		if !letter && (i == 0 || !(c >= '0' && c <= '9' || c == '.' || c == '-')) {
			return "[invalid]"
		}
	}
	return text
}

func integrationDiagnosticKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64, int, json.Number:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "other"
	}
}

func integrationDiagnosticCount(count int) int {
	if count > 1024 {
		return 1024
	}
	return count
}

type ephemeralIntegrationToolNode struct {
	Source, Kind, Type, Name, Namespace, Parent, Role string
	Depth, Fields, Children                           int
	Present                                           []string
	ToolsKind, DeferKind                              string
	Deferred                                          bool
}

type ephemeralIntegrationToolDiagnostic struct {
	Predicate, ToolsKind, InputKind                         string
	Parsed, Required, TopTools, InputItems, AdditionalTools int
	Clock, Async, Question, Truncated                       bool
	Nodes                                                   []ephemeralIntegrationToolNode
}

func integrationToolDiagnostic(request map[string]any, names map[string]bool, failure error, required int) string {
	result := ephemeralIntegrationToolDiagnostic{Predicate: "required-tool-set-mismatch", ToolsKind: integrationDiagnosticKind(request["tools"]), InputKind: integrationDiagnosticKind(request["input"]),
		Parsed: integrationDiagnosticCount(len(names)), Required: required, Clock: names["clock.curr_time"], Async: names["request_user_input_async"], Question: names["request_user_input"]}
	if failure != nil {
		switch failure.Error() {
		case "missing tools", "malformed tool", "unnamed tool", "unexpected namespace", "unexpected tool representation", "action tool exposed", "duplicate tool", "missing input", "malformed input", "unexpected tool role", "missing tool schemas":
			result.Predicate = strings.ReplaceAll(failure.Error(), " ", "-")
		default:
			result.Predicate = "other-predicate-error"
		}
	}
	enum := func(value any, role bool) string {
		if text, ok := value.(string); ok {
			if role {
				switch text {
				case "developer", "system", "assistant", "user", "tool":
					return text
				}
			} else {
				switch text {
				case "function", "namespace", "custom", "additional_tools", "tool_search", "web_search", "file_search", "computer", "shell", "code_interpreter", "image_generation":
					return text
				}
			}
			return "other-string"
		}
		return integrationDiagnosticKind(value)
	}
	var visit func(any, string, string, string, int)
	visit = func(value any, source, parent, role string, depth int) {
		if len(result.Nodes) >= 32 || depth > 4 {
			result.Truncated = true
			return
		}
		node := ephemeralIntegrationToolNode{Source: source, Parent: parent, Role: role, Depth: depth, Kind: integrationDiagnosticKind(value)}
		tool, ok := value.(map[string]any)
		if ok {
			node.Type, node.Fields = enum(tool["type"], false), integrationDiagnosticCount(len(tool))
			for _, key := range []string{"type", "name", "namespace", "role", "tools", "function", "description", "parameters", "arguments", "defer_loading"} {
				if _, exists := tool[key]; exists {
					node.Present = append(node.Present, key)
				}
			}
			if name, exists := tool["name"]; exists {
				node.Name = integrationDiagnosticIdentifier(name)
			}
			if namespace, exists := tool["namespace"]; exists {
				node.Namespace = integrationDiagnosticIdentifier(namespace)
			}
			if itemRole, exists := tool["role"]; exists {
				node.Role = enum(itemRole, true)
			}
			node.ToolsKind, node.DeferKind = integrationDiagnosticKind(tool["tools"]), integrationDiagnosticKind(tool["defer_loading"])
			node.Deferred, _ = tool["defer_loading"].(bool)
			if children, ok := tool["tools"].([]any); ok {
				node.Children = integrationDiagnosticCount(len(children))
			}
		}
		result.Nodes = append(result.Nodes, node)
		if !ok {
			return // An array in place of a tool is opaque, never recursively walked.
		}
		if children, ok := tool["tools"].([]any); ok {
			if depth == 4 && len(children) > 0 {
				result.Truncated = true
				return
			}
			for _, child := range children {
				if len(result.Nodes) >= 32 {
					result.Truncated = true
					break
				}
				visit(child, source, node.Name, node.Role, depth+1)
			}
		}
		if child, exists := tool["function"]; exists {
			visit(child, "wrapped-function", node.Name, node.Role, depth+1)
		}
	}
	if tools, ok := request["tools"].([]any); ok {
		result.TopTools = integrationDiagnosticCount(len(tools))
		for _, tool := range tools {
			if len(result.Nodes) >= 32 {
				result.Truncated = true
				break
			}
			visit(tool, "tools", "", "", 0)
		}
	}
	if input, ok := request["input"].([]any); ok {
		result.InputItems = integrationDiagnosticCount(len(input))
		for i, value := range input {
			if i >= 64 {
				result.Truncated = true
				break
			}
			if item, ok := value.(map[string]any); ok && item["type"] == "additional_tools" {
				result.AdditionalTools++
				visit(item, "additional_tools", "", "", 0)
			}
		}
	}
	for {
		output, err := json.Marshal(result)
		if err == nil && len(output) <= 8192 {
			return string(output)
		}
		if len(result.Nodes) == 0 {
			return `{"Predicate":"diagnostic-bound","Truncated":true}`
		}
		result.Truncated = true
		result.Nodes = result.Nodes[:len(result.Nodes)-1]
	}
}

func integrationToolNames(request map[string]any, questionControl bool) (map[string]bool, error) {
	names := map[string]bool{}
	tools, ok := request["tools"].([]any)
	if !ok {
		return names, errors.New("missing tools")
	}
	for _, value := range tools {
		tool, ok := value.(map[string]any)
		if !ok {
			return names, errors.New("malformed tool")
		}
		if _, present := tool["namespace"]; present {
			return names, errors.New("unexpected namespace")
		}
		if _, present := tool["tools"]; present {
			return names, errors.New("unexpected namespace")
		}
		if value, present := tool["defer_loading"]; present && value != false {
			return names, errors.New("unexpected tool representation")
		}
		if tool["type"] != "function" {
			return names, errors.New("unexpected tool representation")
		}
		name, ok := tool["name"].(string)
		if !ok || name == "" {
			return names, errors.New("unnamed tool")
		}
		if !questionControl || name != "request_user_input" {
			return names, errors.New("action tool exposed")
		}
		if names[name] {
			return names, errors.New("duplicate tool")
		}
		names[name] = true
	}
	input, ok := request["input"].([]any)
	if !ok {
		return names, errors.New("missing input")
	}
	for _, value := range input {
		item, ok := value.(map[string]any)
		if !ok {
			return names, errors.New("malformed input")
		}
		if item["type"] == "additional_tools" {
			return names, errors.New("unexpected tool role")
		}
		if _, present := item["tools"]; present {
			return names, errors.New("unexpected tool role")
		}
	}
	if _, present := request["additional_tools"]; present {
		return names, errors.New("unexpected tool role")
	}
	return names, nil
}

func TestEphemeralIntegrationToolSchemaShapes(t *testing.T) {
	question := map[string]any{"type": "function", "name": "request_user_input"}
	for _, test := range []struct {
		name           string
		tools          any
		input          []any
		control, valid bool
	}{
		{"normal Direct", []any{}, []any{}, false, true},
		{"labelled server question", []any{question}, []any{}, true, true},
		{"missing", nil, []any{}, false, false},
		{"question forbidden normally", []any{question}, []any{}, false, false},
		{"duplicate", []any{question, question}, []any{}, true, false},
		{"action", []any{map[string]any{"type": "function", "name": "exec_command"}}, []any{}, false, false},
		{"hosted", []any{map[string]any{"type": "web_search"}}, []any{}, false, false},
		{"wrapper", []any{map[string]any{"type": "namespace", "name": "functions", "tools": []any{map[string]any{"type": "custom", "name": "exec"}}}}, []any{}, false, false},
		{"clock", []any{map[string]any{"type": "namespace", "name": "clock", "tools": []any{map[string]any{"type": "function", "name": "curr_time"}}}}, []any{}, false, false},
		{"async", []any{map[string]any{"type": "function", "name": "request_user_input_async"}}, []any{}, false, false},
		{"deferred", []any{map[string]any{"type": "function", "name": "request_user_input", "defer_loading": true}}, []any{}, true, false},
		{"Lite developer", []any{}, []any{map[string]any{"type": "additional_tools", "role": "developer", "tools": []any{}}}, false, false},
		{"hidden input schema", []any{}, []any{map[string]any{"role": "user", "tools": []any{}}}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			names, err := integrationToolNames(map[string]any{"tools": test.tools, "input": test.input}, test.control)
			if test.valid {
				if err != nil || len(names) != len(test.tools.([]any)) {
					t.Fatal("known Direct schema rejected", err)
				}
			} else if err == nil {
				t.Fatal("unexpected model-facing schema accepted")
			}
		})
	}
}

func TestEphemeralIntegrationToolDiagnostics(t *testing.T) {
	t.Run("shape and predicate category without schema data", func(t *testing.T) {
		tool := map[string]any{"type": "custom", "name": "exec", "namespace": "functions", "defer_loading": true,
			"description": "DESCRIPTION_SECRET", "parameters": map[string]any{"AUTH_SECRET": "VALUE_SECRET"}, "arguments": "ARGUMENT_SECRET"}
		request := map[string]any{"tools": []any{}, "input": []any{map[string]any{"type": "message", "content": "PROMPT_SECRET"}, map[string]any{"type": "additional_tools", "role": "developer", "tools": []any{tool}}}}
		names, predicate := integrationToolNames(request, false)
		if predicate == nil {
			t.Fatal("existing unsupported-tool predicate must still fail")
		}
		output := integrationToolDiagnostic(request, names, predicate, 0)
		var got ephemeralIntegrationToolDiagnostic
		if json.Unmarshal([]byte(output), &got) != nil || got.Predicate != "unexpected-tool-role" || got.AdditionalTools != 1 || len(got.Nodes) != 2 {
			t.Fatal("missing sanitized mismatch shape")
		}
		node := got.Nodes[1]
		if node.Type != "custom" || node.Name != "exec" || node.Namespace != "functions" || node.Role != "developer" || !node.Deferred || node.DeferKind != "boolean" {
			t.Fatal("known scalar shape was lost")
		}
		for _, secret := range []string{"DESCRIPTION_SECRET", "AUTH_SECRET", "VALUE_SECRET", "ARGUMENT_SECRET", "PROMPT_SECRET"} {
			if strings.Contains(output, secret) {
				t.Fatal("tool diagnostic disclosed opaque data")
			}
		}
	})
	t.Run("invalid scalars and errors are fixed categories", func(t *testing.T) {
		request := map[string]any{"tools": []any{map[string]any{"name": "unsafe/NAME_SECRET", "namespace": strings.Repeat("NAMESPACE_SECRET", 16), "type": "TYPE_SECRET", "role": "ROLE_SECRET", "defer_loading": "DEFER_SECRET"}}, "input": []any{}}
		output := integrationToolDiagnostic(request, nil, errors.New("ERROR_SECRET"), 2)
		var got ephemeralIntegrationToolDiagnostic
		if json.Unmarshal([]byte(output), &got) != nil || got.Predicate != "other-predicate-error" || got.Nodes[0].Name != "[invalid]" || got.Nodes[0].Namespace != "[invalid]" || got.Nodes[0].Type != "other-string" || got.Nodes[0].Role != "other-string" || got.Nodes[0].DeferKind != "string" {
			t.Fatal("unsafe diagnostic scalar accepted")
		}
		if strings.Contains(output, "SECRET") {
			t.Fatal("invalid diagnostic data leaked")
		}
	})
	t.Run("width depth input and aggregate limits", func(t *testing.T) {
		leaf := any(map[string]any{"type": "function", "name": strings.Repeat("x", 64), "namespace": strings.Repeat("y", 64), "description": "SECRET"})
		deep := leaf
		for i := 0; i < 100; i++ {
			deep = map[string]any{"type": "namespace", "name": "nested", "tools": []any{deep}}
		}
		tools := []any{deep, []any{[]any{leaf}}}
		for i := 0; i < 100; i++ {
			tools = append(tools, leaf)
		}
		input := make([]any, 100)
		for i := range input {
			input[i] = map[string]any{"type": "additional_tools", "role": "developer", "tools": []any{leaf}}
		}
		output := integrationToolDiagnostic(map[string]any{"tools": tools, "input": input}, nil, nil, 2)
		var got ephemeralIntegrationToolDiagnostic
		if len(output) > 8192 || json.Unmarshal([]byte(output), &got) != nil || !got.Truncated || len(got.Nodes) > 32 || got.AdditionalTools != 64 {
			t.Fatal("diagnostic bounds lost")
		}
		for _, node := range got.Nodes {
			if node.Depth > 4 {
				t.Fatal("unbounded diagnostic traversal")
			}
		}
		if strings.Contains(output, "SECRET") {
			t.Fatal("bounded traversal exposed descriptions")
		}
	})
}

func (h *ephemeralIntegrationHarness) ordinaryClient() *Client {
	return NewWithOptions(h.socket, ClientOptions{DeveloperInstructions: "TEAM_POLICY_SENTINEL", RuntimeWorkspaceRoots: []string{h.project}})
}

func (h *ephemeralIntegrationHarness) retainedControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result struct {
		Thread struct {
			ID    string                        `json:"id"`
			Turns []struct{ ID, Status string } `json:"turns"`
		} `json:"thread"`
	}
	if h.ordinary.Request(ctx, "thread/read", map[string]any{"threadId": h.controlThread, "includeTurns": true}, &result) != nil ||
		result.Thread.ID != h.controlThread || len(result.Thread.Turns) == 0 ||
		result.Thread.Turns[len(result.Thread.Turns)-1].ID != h.controlTurn ||
		result.Thread.Turns[len(result.Thread.Turns)-1].Status != "completed" {
		t.Fatal("persisted ordinary control continuity unavailable")
	}
}

func (h *ephemeralIntegrationHarness) control(t *testing.T) {
	h.trustControlHooks(t)
	h.mu.Lock()
	h.controlMode = true
	h.requests = nil
	h.controlUpgrades = 0
	h.failure = ""
	h.mu.Unlock()
	h.ordinary = h.ordinaryClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cwd := filepath.Join(h.project, "work/session")
	config := map[string]any{"shell_environment_policy": map[string]any{"set": map[string]string{}}}
	params := map[string]any{"cwd": cwd, "config": config}
	if err := h.ordinary.addRuntimeWorkspaceRoots(params); err != nil {
		t.Fatalf("control thread prerequisite unavailable: %v", err)
	}
	h.ordinary.settingsParams(ThreadSettings{Model: "gpt-6-luna", ReasoningEffort: "low"}, params, config)
	// Use the ordinary client's existing builders and exact start shape, while
	// retaining native source metadata that StartThreadWithSettings discards.
	var started struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
		InstructionSources []string `json:"instructionSources"`
	}
	if err := h.ordinary.Request(ctx, "thread/start", params, &started); err != nil {
		t.Fatalf("control thread prerequisite unavailable: %v", err)
	}
	id := started.Thread.ID
	if id == "" || started.Thread.Cwd != cwd {
		t.Fatal("control thread/start returned no identity or the wrong working directory")
	}
	h.controlThread = id
	var turn struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := h.ordinary.Request(ctx, "turn/start", map[string]any{"threadId": id, "model": "gpt-6-luna", "effort": "low", "input": []any{map[string]any{"type": "text", "text": integrationControlInput}}}, &turn); err != nil {
		t.Fatalf("control inference prerequisite unavailable: %v", err)
	}
	if turn.Turn.ID == "" {
		t.Fatal("control turn/start returned no turn identity")
	}
	h.controlTurn = turn.Turn.ID
	for ctx.Err() == nil {
		h.mu.Lock()
		received := false
		for _, request := range h.requests {
			if integrationControlRequest(request, id, turn.Turn.ID) {
				received = true
				break
			}
		}
		h.mu.Unlock()
		if received {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.mu.Lock()
	controlRequests := []map[string]any{}
	for _, request := range h.requests {
		if integrationControlRequest(request, id, turn.Turn.ID) {
			controlRequests = append(controlRequests, request)
		}
	}
	controlUpgrades := h.controlUpgrades
	h.controlMode = false
	h.mu.Unlock()
	if len(controlRequests) == 0 {
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), time.Second)
		defer diagnosticCancel()
		var diagnostic struct {
			Thread struct {
				Turns json.RawMessage `json:"turns"`
			} `json:"thread"`
		}
		diagnosticErr := h.ordinary.Request(diagnosticCtx, "thread/read", map[string]any{"threadId": id, "includeTurns": true}, &diagnostic)
		turns := diagnostic.Thread.Turns
		if len(turns) > 4096 {
			turns = turns[:4096]
		}
		t.Fatalf("representative builtin OpenAI HTTP-fallback did not reach the exact control turn POST (accepted upgrades: %d; control: %v; thread/read: %v; synthetic turns: %s; control evidence: %s; app-server stderr: %s)", controlUpgrades, ctx.Err(), diagnosticErr, turns, h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
	}
	if controlUpgrades == 0 {
		t.Fatal("control did not exercise native builtin OpenAI upgrade/HTTP fallback")
	}
	t.Logf("native builtin OpenAI HTTP-fallback control: %d accepted upgrades; %d decoded Responses POSTs", controlUpgrades, len(controlRequests))
	encoded, _ := json.Marshal(controlRequests)
	for _, sentinel := range integrationControlSentinels {
		if !bytes.Contains(encoded, []byte(sentinel)) {
			t.Fatalf("control did not prove %s eligibility; fixture cannot claim its exclusion (control evidence: %s; app-server stderr: %s)", sentinel, h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
		}
	}
	for _, request := range controlRequests {
		reasoning, _ := request["reasoning"].(map[string]any)
		if request["model"] != "gpt-6-luna" || reasoning["effort"] != "low" {
			t.Fatalf("control has substituted model/effort (control evidence: %s; app-server stderr: %s)", h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
		}
	}
	if !bytes.Contains(encoded, []byte("exec_command")) || !bytes.Contains(encoded, []byte("curr_time")) || !bytes.Contains(encoded, []byte("request_user_input_async")) {
		t.Fatalf("control has reduced tool/model metadata (control evidence: %s; app-server stderr: %s)", h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
	}
	// Completion proves the original eligibility control, but does not join its
	// detached snapshot/memory writers. The namespace barrier below must do so.
	for ctx.Err() == nil {
		var result struct {
			Thread struct {
				Turns []struct {
					Status string `json:"status"`
				} `json:"turns"`
			} `json:"thread"`
		}
		if h.ordinary.Request(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": true}, &result) != nil {
			t.Fatal("control read failed")
		}
		if len(result.Thread.Turns) > 0 && result.Thread.Turns[len(result.Thread.Turns)-1].Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("control did not complete")
	}
	h.egressDrops = h.droppedPackets(t)
	h.ordinary.Close()
	h.ordinary = nil
	h.stop()
	h.start(t)
	h.ordinary = h.ordinaryClient()
	// Read unloaded persisted state only: no resume/start/new ordinary turn can
	// restart eligibility writers after this one-time quiescence barrier.
	h.retainedControl(t)
	if h.droppedPackets(t) != h.egressDrops {
		t.Fatal("eligibility barrier restart caused denied egress")
	}
	h.seedSensitiveStores(t)
	h.baseline = h.snapshot(t)
}

func (h *ephemeralIntegrationHarness) trustControlHooks(t *testing.T) {
	// Discover and trust only the synthetic disposable hooks through the pinned
	// public list shape and ordinary config hash state, never bypass hook trust.
	client := New(h.socket)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result struct {
		Data []struct {
			Hooks []struct {
				Key         string `json:"key"`
				CurrentHash string `json:"currentHash"`
				Source      string `json:"source"`
			} `json:"hooks"`
		} `json:"data"`
	}
	err := client.Request(ctx, "hooks/list", map[string]any{"cwds": []string{filepath.Join(h.project, "work/session")}}, &result)
	client.Close()
	if err != nil {
		t.Fatal("hook discovery prerequisite unavailable")
	}
	sources := map[string]bool{}
	count := 0
	for _, entry := range result.Data {
		for _, hook := range entry.Hooks {
			if !ephemeralID(hook.Key) || !ephemeralID(hook.CurrentHash) || count >= 16 {
				t.Fatal("bounded synthetic hook inventory invalid")
			}
			sources[hook.Source] = true
			count++
			h.config += "\n[hooks.state." + strconv.Quote(hook.Key) + "]\ntrusted_hash = " + strconv.Quote(hook.CurrentHash) + "\n"
		}
	}
	if !sources["user"] || !sources["plugin"] {
		t.Fatal("control cannot prove user and plugin hook eligibility")
	}
	h.stop()
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
	h.start(t)
}

func (h *ephemeralIntegrationHarness) seedSensitiveStores(t *testing.T) {
	// These are the actual pinned-source stores created by the candidate, not
	// invented sentinel files. A schema/prerequisite mismatch fails the fixture.
	sqlite := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	for _, seed := range []struct{ name, statement, verify string }{
		{"goals_1.sqlite", "INSERT INTO thread_goals (thread_id,goal_id,objective,status,created_at_ms,updated_at_ms) VALUES (" + quote(h.controlThread) + ",'00000000-0000-4000-8000-000000000001','GOAL_STATE_SENTINEL','paused',1,1);",
			"SELECT count(*) FROM thread_goals WHERE thread_id=" + quote(h.controlThread) + " AND goal_id='00000000-0000-4000-8000-000000000001' AND objective='GOAL_STATE_SENTINEL' AND status='paused' AND created_at_ms=1 AND updated_at_ms=1;"},
		{"memories_1.sqlite", "INSERT INTO stage1_outputs (thread_id,source_updated_at,raw_memory,rollout_summary,generated_at) VALUES ('00000000-0000-4000-8000-000000000002',1,'MEMORY_POLICY_SENTINEL','synthetic retained memory',1);",
			"SELECT count(*) FROM stage1_outputs WHERE thread_id='00000000-0000-4000-8000-000000000002' AND source_updated_at=1 AND raw_memory='MEMORY_POLICY_SENTINEL' AND rollout_summary='synthetic retained memory' AND generated_at=1;"},
	} {
		path := filepath.Join(h.home, seed.name)
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			t.Fatal("candidate sensitive-store prerequisite missing")
		}
		if _, err := exec.Command(sqlite, path, seed.statement).Output(); err != nil {
			t.Fatal("candidate sensitive-store seed failed")
		}
		if output, err := exec.Command(sqlite, path, seed.verify).Output(); err != nil || string(output) != "1\n" {
			t.Fatal("candidate sensitive-store seed equality failed")
		}
	}
	if count, err := os.ReadFile(filepath.Join(h.home, "mcp-count")); err != nil || len(count) == 0 {
		t.Fatal("control did not prove subprocess MCP startup eligibility")
	}
	if count, err := os.ReadFile(filepath.Join(h.home, "hook-count")); err != nil || len(count) == 0 {
		t.Fatal("control did not prove hook/notify eligibility")
	}
}

func (h *ephemeralIntegrationHarness) proxy(t *testing.T, mode string, generation uint64) string {
	t.Helper()
	return serveUnixWebsocket(t, func(downstream *websocket.Conn) error {
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			socket := h.utilitySocket
			if mode == "dynamic-catalog" {
				socket = h.socket
			}
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer downstream.CloseNow()
		upstream, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
		if err != nil {
			return errors.New("candidate proxy connection failed")
		}
		defer upstream.CloseNow()
		var mu sync.Mutex
		methods := map[string]string{}
		var held []byte
		var threadID, turnID string
		var inventory []string
		var questionID string
		errorsChannel := make(chan error, 2)
		go func() {
			for {
				kind, raw, err := downstream.Read(ctx)
				if err != nil {
					errorsChannel <- nil
					return
				}
				var message rpcMessage
				if json.Unmarshal(raw, &message) != nil {
					errorsChannel <- errors.New("malformed client envelope")
					return
				}
				mu.Lock()
				if message.Method != "" && len(message.ID) > 0 {
					methods[string(message.ID)] = message.Method
				}
				if message.Method == "thread/start" {
					var params map[string]any
					_ = json.Unmarshal(message.Params, &params)
					policy, _ := params["config"].(map[string]any)
					servers, _ := policy["mcp_servers"].(map[string]any)
					valid := len(servers) == len(inventory)
					for _, name := range inventory {
						entry, ok := servers[name].(map[string]any)
						if !ok || entry["enabled"] != false {
							valid = false
						}
					}
					if !valid {
						h.mu.Lock()
						h.failure = "captured literal MCP names not all disabled"
						h.mu.Unlock()
					}
				}
				if message.Method == "turn/interrupt" {
					var params map[string]any
					_ = json.Unmarshal(message.Params, &params)
					if threadID != "" && turnID != "" && params["threadId"] == threadID && params["turnId"] == turnID {
						h.mu.Lock()
						h.interrupted = true
						h.mu.Unlock()
					} else {
						h.mu.Lock()
						h.failure = "cleanup interrupted an unowned identity"
						h.mu.Unlock()
					}
				}
				questionError := string(message.ID) == questionID && questionID != "" && message.Error != nil
				h.captureRPC(generation, "request", message.Method, nil, threadID, turnID)
				mu.Unlock()
				if questionError {
					if message.Error.Code == -32601 && message.Error.Message == "Questions are unavailable for ephemeral utility turns" {
						h.proxyError.Store(true)
					}
				}
				// Only this labelled exact-binary control enables the otherwise disabled
				// blocking input tool. Production policy/API has no such escape hatch.
				if (mode == "server-question-control" || mode == "question-at-cancel") && (message.Method == "thread/start" || message.Method == "turn/start") {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					if message.Method == "thread/start" {
						policy := params["config"].(map[string]any)
						policy["tools.experimental_request_user_input.enabled"] = true
					}
					if message.Method == "turn/start" {
						params["collaborationMode"] = map[string]any{"mode": "plan", "settings": map[string]any{"model": "gpt-5.5", "reasoning_effort": "low", "developer_instructions": nil}}
					}
					raw, _ = json.Marshal(object)
				}
				if err := upstream.Write(ctx, kind, raw); err != nil {
					errorsChannel <- nil
					return
				}
			}
		}()
		go func() {
			for {
				kind, raw, err := upstream.Read(ctx)
				if err != nil {
					errorsChannel <- nil
					return
				}
				var message rpcMessage
				if json.Unmarshal(raw, &message) != nil {
					errorsChannel <- errors.New("malformed candidate envelope")
					return
				}
				mu.Lock()
				method := ""
				if message.Method == "" && len(message.ID) > 0 {
					method = methods[string(message.ID)]
				}
				withhold := (mode == "timeout-config" && method == "config/read") || (mode == "timeout-model" && method == "model/list")
				if withhold {
					// This is the actual upstream reply to this proxy's issued request,
					// not an inferred timeout or a callback from another helper call.
					h.noteWithheldResponse(generation, method, message)
				}
				if method != "" {
					direction := "response"
					if withhold {
						direction = "withheld-response"
					}
					h.captureRPC(generation, direction, method, message.Error, threadID, turnID)
					delete(methods, string(message.ID))
				}
				if method == "config/read" && message.Error == nil {
					var result struct {
						Config map[string]any `json:"config"`
					}
					_ = json.Unmarshal(message.Result, &result)
					if mode != "dynamic-catalog" && h.catalog != "" {
						_, originErr := ephemeralRestrictions(message.Result, "/private/instructions.txt", h.catalog)
						h.mu.Lock()
						if generation == h.diagnosticGeneration {
							h.catalogConfirmed = originErr == nil
						}
						h.mu.Unlock()
					}
					servers, _ := result.Config["mcp_servers"].(map[string]any)
					for name := range servers {
						inventory = append(inventory, name)
					}
				}
				if withhold {
					mu.Unlock()
					continue
				}
				if method == "thread/start" && message.Error == nil {
					var result struct {
						Thread struct {
							ID string `json:"id"`
						} `json:"thread"`
					}
					_ = json.Unmarshal(message.Result, &result)
					threadID = result.Thread.ID
					h.captureRPC(generation, "owned", "thread/start", nil, threadID, "")
					h.mu.Lock()
					h.threadIDs = append(h.threadIDs, threadID)
					h.mu.Unlock()
					if mode == "cancel-start" {
						mu.Unlock()
						continue
					}
				}
				if method == "turn/start" && message.Error == nil {
					var result struct {
						Turn struct {
							ID string `json:"id"`
						} `json:"turn"`
					}
					_ = json.Unmarshal(message.Result, &result)
					turnID = result.Turn.ID
					h.captureRPC(generation, "owned", "turn/start", nil, threadID, turnID)
					if mode == "early-events" {
						held = append([]byte(nil), raw...)
						mu.Unlock()
						continue
					}
					if mode == "lost-turn" {
						mu.Unlock()
						continue
					}
				}
				if message.Method == "turn/started" {
					var params struct {
						ThreadID string `json:"threadId"`
						Turn     struct {
							ID string `json:"id"`
						} `json:"turn"`
					}
					_ = json.Unmarshal(message.Params, &params)
					if params.ThreadID == threadID && turnID == "" {
						turnID = params.Turn.ID
					}
				}
				if message.Method == "item/started" || message.Method == "item/completed" {
					var params struct {
						ThreadID string `json:"threadId"`
						TurnID   string `json:"turnId"`
					}
					_ = json.Unmarshal(message.Params, &params)
					if params.ThreadID == threadID && turnID == "" {
						turnID = params.TurnID
					}
				}
				if message.Method == "item/tool/requestUserInput" && len(message.ID) > 0 {
					questionID = string(message.ID)
				}
				if message.Method != "" {
					h.captureRPC(generation, "notification", message.Method, nil, threadID, turnID)
				}
				disconnect := mode == "disconnect" && method == "turn/start"
				ambiguous := mode == "ambiguous" && message.Method == "turn/completed"
				release := message.Method == "turn/completed" && len(held) > 0
				delayed := append([]byte(nil), held...)
				if release {
					held = nil
				}
				var questionCancel context.CancelFunc
				if mode == "question-at-cancel" {
					questionCancel = h.noteQuestionAtCancel(generation, message, threadID, turnID)
				}
				mu.Unlock()
				if disconnect {
					upstream.CloseNow()
					errorsChannel <- nil
					return
				}
				if questionCancel != nil {
					questionCancel()
				}
				if mode == "malformed" && message.Method == "turn/completed" {
					raw = []byte("{")
				}
				if mode == "stale" && message.Method == "item/completed" {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					params["turnId"] = "stale-turn"
					raw, _ = json.Marshal(object)
				}
				if mode == "missing-final" && message.Method == "item/completed" {
					continue
				}
				if mode == "missing-final" && message.Method == "turn/completed" {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					turn := params["turn"].(map[string]any)
					turn["items"] = []any{}
					raw, _ = json.Marshal(object)
				}
				if ambiguous {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params, _ := object["params"].(map[string]any)
					turn, _ := params["turn"].(map[string]any)
					items, _ := turn["items"].([]any)
					turn["items"] = append(items, map[string]any{"type": "agentMessage", "id": "second-answer", "phase": "final_answer", "text": "ambiguous"})
					raw, _ = json.Marshal(object)
				}
				if err := downstream.Write(ctx, kind, raw); err != nil {
					errorsChannel <- nil
					return
				}
				if release {
					if downstream.Write(ctx, kind, delayed) != nil {
						errorsChannel <- nil
						return
					}
				}
			}
		}()
		err = <-errorsChannel
		cancel()
		upstream.CloseNow()
		downstream.CloseNow()
		<-errorsChannel
		return err
	})
}

func (h *ephemeralIntegrationHarness) noteQuestionAtCancel(generation uint64, message rpcMessage, threadID, turnID string) context.CancelFunc {
	if message.Method != "item/tool/requestUserInput" || len(message.ID) == 0 || len(message.ID) > 256 || !ephemeralID(threadID) || !ephemeralID(turnID) || len(message.Params) > 256*1024 {
		return nil
	}
	var id any
	if json.Unmarshal(message.ID, &id) != nil {
		return nil
	}
	switch value := id.(type) {
	case string:
		if value == "" {
			return nil
		}
	case float64:
	default:
		return nil
	}
	var params struct {
		ThreadID  string            `json:"threadId"`
		TurnID    string            `json:"turnId"`
		Questions []json.RawMessage `json:"questions"`
	}
	if json.Unmarshal(message.Params, &params) != nil || params.ThreadID != threadID || params.TurnID != turnID || len(params.Questions) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if generation != h.diagnosticGeneration || h.mode != "question-at-cancel" || !h.diagnosticCallActive || h.diagnosticCallContext == nil || h.diagnosticCallContext.Err() != nil || h.activeCancel == nil || h.questionObserved || !h.questionDispatched || len(h.requests) == 0 {
		return nil
	}
	h.questionObserved = true
	return h.activeCancel
}

func TestEphemeralIntegrationQuestionCancellationTarget(t *testing.T) {
	for _, mode := range []string{"matching request", "no ID", "null ID", "boolean ID", "async item", "wrong thread", "wrong turn", "no questions", "foreign leaf", "inactive", "no POST", "no dispatch", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			count := 0
			h := &ephemeralIntegrationHarness{mode: "question-at-cancel", diagnosticGeneration: 7, diagnosticCallContext: ctx, diagnosticCallActive: true, questionDispatched: true, requests: []map[string]any{{}}, activeCancel: func() { count++ }}
			message := rpcMessage{Method: "item/tool/requestUserInput", ID: json.RawMessage(`"q"`)}
			params := map[string]any{"threadId": "owned-thread", "turnId": "owned-turn", "questions": []any{map[string]any{"id": "q"}}}
			switch mode {
			case "no ID":
				message.ID = nil
			case "null ID":
				message.ID = json.RawMessage(`null`)
			case "boolean ID":
				message.ID = json.RawMessage(`true`)
			case "async item":
				message.Method = "item/started"
			case "wrong thread":
				params["threadId"] = "foreign"
			case "wrong turn":
				params["turnId"] = "foreign"
			case "no questions":
				params["questions"] = []any{}
			case "foreign leaf":
				h.diagnosticGeneration++
			case "inactive":
				h.diagnosticCallActive = false
			case "no POST":
				h.requests = nil
			case "no dispatch":
				h.questionDispatched = false
			case "cancelled":
				cancel()
			}
			message.Params, _ = json.Marshal(params)
			callback := h.noteQuestionAtCancel(7, message, "owned-thread", "owned-turn")
			if (callback != nil) != (mode == "matching request") {
				t.Fatal("wrong server-ID cancellation target")
			}
			if callback != nil {
				callback()
				if count != 1 || !h.questionObserved || h.noteQuestionAtCancel(7, message, "owned-thread", "owned-turn") != nil {
					t.Fatal("question proof not one-shot")
				}
			}
		})
	}
}

func (h *ephemeralIntegrationHarness) run(t *testing.T, mode, namespace, attack string) {
	h.mu.Lock()
	h.mode, h.namespace, h.attack = mode, namespace, attack
	h.requests = nil
	h.utilityUpgrades = 0
	h.failure = ""
	h.heldContinuation = make(chan struct{})
	h.continuationStopped = make(chan struct{})
	h.interrupted = false
	h.catalogConfirmed = false
	h.refreshDelivered = false
	h.mu.Unlock()
	generation := h.beginDiagnostics()
	defer func() {
		h.mu.Lock()
		if generation == h.diagnosticGeneration {
			h.diagnosticsActive = false
		}
		h.mu.Unlock()
	}()
	h.capturePhase(generation, "before-utility", "boundary", "none", nil, "", "")
	h.proxyError.Store(false)
	client := New(h.proxy(t, mode, generation))
	defer client.Close()
	opts := ephemeralTestOptions(t)
	opts.ModelCatalogFile = h.catalog
	// Provision the provider-owned nonempty file contract used by real callers.
	content, fileErr := os.ReadFile(opts.InstructionFile)
	if fileErr != nil || string(content) != EphemeralInstructionFileContent {
		t.Fatal("native fixture instruction file differs from public contract")
	}
	opts.Input = "RAW_UTILITY_PREFIX_SENTINEL: fix runtime session naming"
	duration := 3 * time.Second
	if mode == "cancel-start" || strings.HasPrefix(mode, "timeout") || mode == "missing-final" {
		duration = 400 * time.Millisecond
	}
	if mode == "lost-turn" {
		duration = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	h.mu.Lock()
	h.activeCancel = cancel
	h.mu.Unlock()
	defer func() { h.mu.Lock(); h.activeCancel = nil; h.mu.Unlock() }()
	// Keep an ordinary connection active while the private helper runs. It must
	// retain its generation/settings and never acquire utility prompts/notices.
	h.ordinary.connectionMu.Lock()
	ordinaryGeneration := h.ordinary.generation
	h.ordinary.connectionMu.Unlock()
	ordinaryDone := make(chan error, 1)
	go func() {
		var result any
		ordinaryDone <- h.ordinary.Request(ctx, "thread/read", map[string]any{"threadId": h.controlThread, "includeTurns": false}, &result)
	}()
	h.mu.Lock()
	h.diagnosticCallContext, h.diagnosticCallActive = ctx, true
	h.mu.Unlock()
	result, err := client.RunEphemeralTurn(ctx, opts)
	h.mu.Lock()
	h.diagnosticCallActive = false
	h.mu.Unlock()
	h.capturePhase(generation, "after-utility-teardown", "boundary", "none", nil, "", "")
	h.mu.Lock()
	phases := append([]ephemeralIntegrationPhase(nil), h.phases...)
	toolDiagnostic := h.toolDiagnostic
	h.mu.Unlock()
	// Emit before any category assertion so setup failures retain their evidence.
	evidence, _ := json.Marshal(phases)
	t.Logf("native utility phase evidence (fixed deny baseline=%d; ordinary thread=%s): %s", h.egressDrops, h.controlThread, evidence)
	if toolDiagnostic != "" {
		t.Logf("native tool-schema mismatch metadata: %s", toolDiagnostic)
	}
	if ordinaryErr := <-ordinaryDone; ordinaryErr != nil {
		t.Fatal("ordinary connection disrupted")
	}
	h.ordinary.connectionMu.Lock()
	same := h.ordinary.generation == ordinaryGeneration
	h.ordinary.connectionMu.Unlock()
	if !same || len(h.ordinary.Prompts(h.controlThread)) != 0 {
		t.Fatal("ordinary state contaminated")
	}
	if (mode == "timeout-config" || mode == "timeout-model") && !h.timeoutFaultExercised(generation, mode) {
		t.Fatal("designated native response was not withheld while this helper call was active")
	}
	switch mode {
	case "server-question-control", "dynamic-catalog":
		if !errors.Is(err, ErrEphemeralIsolation) {
			t.Fatalf("question accepted: result=%#v error=%v", result, err)
		}
	case "cancel-start", "timeout-config", "timeout-model", "timeout", "missing-final", "lost-turn":
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("budget not preserved: %v", err)
		}
	case "disconnect":
		if !errors.Is(err, ErrEphemeralServer) {
			t.Fatalf("disconnect adopted: %v", err)
		}
	case "question-at-cancel":
		h.mu.Lock()
		questionProof := h.questionObserved && h.questionDispatched && len(h.requests) > 0
		h.mu.Unlock()
		if !questionProof {
			t.Fatal("matching native server-ID question and provider dispatch were not observed before cancellation")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending question cancellation lost: %v", err)
		}
	case "ambiguous", "stale", "malformed":
		if !errors.Is(err, ErrEphemeralProtocol) {
			t.Fatalf("ambiguous final accepted: %v", err)
		}
	default:
		if err != nil || result.Text != `{"name":"fix runtime session names"}` {
			t.Fatalf("candidate utility did not complete: %v", err)
		}
	}
	if mode == "server-question-control" && !h.proxyError.Load() {
		t.Fatal("actual-ID private request did not receive fixed -32601 error")
	}
	if mode == "lost-turn" {
		h.mu.Lock()
		running, stopped, interrupted := h.heldContinuation, h.continuationStopped, h.interrupted
		h.mu.Unlock()
		select {
		case <-running:
		default:
			t.Fatal("lost-response control never held live inference")
		}
		if !interrupted {
			t.Fatal("lost-response continuation received no exact-turn interrupt")
		}
		select {
		case <-stopped:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("exact interrupt left the actual provider continuation running")
		}
	}
	h.mu.Lock()
	failure := h.failure
	requests := append([]map[string]any(nil), h.requests...)
	utilityUpgrades := h.utilityUpgrades
	confirmed, refresh := h.catalogConfirmed, h.refreshDelivered
	h.mu.Unlock()
	if failure != "" {
		t.Fatal(failure)
	}
	if mode != "dynamic-catalog" && mode != "timeout-config" && !confirmed {
		t.Fatal("actual native startup CLI catalog origin not confirmed")
	}
	if mode == "static-refresh" && !refresh {
		t.Fatal("actual ETag refresh response not delivered")
	}
	if mode != "cancel-start" && mode != "timeout-config" && mode != "timeout-model" && mode != "dynamic-catalog" && len(requests) == 0 {
		t.Fatalf("fixture did not exercise a Responses POST; %d accepted upgrades alone are not proof", utilityUpgrades)
	}
	if len(requests) > 0 && utilityUpgrades == 0 {
		t.Fatal("utility inference did not exercise native builtin OpenAI upgrade/HTTP fallback")
	}
	t.Logf("native builtin OpenAI HTTP-fallback utility %s: %d accepted upgrades; %d decoded Responses POSTs", mode, utilityUpgrades, len(requests))
	data, _ := json.Marshal(requests)
	if bytes.Contains(data, []byte("ACTUAL_FILE_READ_CANARY_8cdbe5")) {
		t.Fatal("forbidden file was read")
	}
	if mode == "baseline" || mode == "early-events" {
		if !bytes.Contains(data, []byte(opts.Input)) || !bytes.Contains(data, []byte("additionalProperties")) || !bytes.Contains(data, []byte(opts.Instructions)) {
			t.Fatal("raw text/schema/explicit instructions absent from actual provider request")
		}
	}
	if mode == "forbidden" {
		if len(requests) < 2 {
			t.Fatal("forbidden call did not exercise actual dispatch")
		}
		parts := strings.SplitN(attack, "\n", 2)
		if !integrationRegistryRejected(requests[1], "call-1", parts[0], namespace) {
			t.Fatal("model followup contains no exact dispatch rejection evidence")
		}

	}
	if content, err := os.ReadFile(h.canary); err != nil || string(content) != "ACTUAL_FILE_READ_CANARY_8cdbe5" {
		t.Fatal("forbidden file write/delete")
	}
	if h.canaryHits.Load() != 0 {
		t.Fatal("forbidden action network request")
	}
	if h.droppedPackets(t) != h.egressDrops {
		t.Fatal("forbidden action attempted denied egress")
	}
	h.verifyPersistence(t)
}

// Pinned tools/registry.rs:853-856 returns these exact finite errors for
// missing handlers. ToolName Display omits the default functions namespace.
func integrationRegistryRejected(request map[string]any, callID, name, namespace string) bool {
	input, ok := request["input"].([]any)
	if !ok {
		return false
	}
	custom := name == "apply_patch" || name == "exec"
	kind, prefix := "function_call_output", "unsupported call: "
	if custom {
		kind, prefix = "custom_tool_call_output", "unsupported custom tool call: "
	}
	display := name
	if namespace != "" && namespace != "functions" {
		display = namespace + name
	}
	for _, value := range input {
		item, ok := value.(map[string]any)
		if !ok || item["type"] != kind || item["call_id"] != callID {
			continue
		}
		output, ok := item["output"].(string)
		if ok && output == prefix+display {
			return true
		}
	}
	return false
}

func TestEphemeralIntegrationRegistryRejection(t *testing.T) {
	for _, test := range []struct {
		name, namespace, kind, output string
		valid                         bool
	}{
		{"exec_command", "", "function_call_output", "unsupported call: exec_command", true},
		{"exec", "functions", "custom_tool_call_output", "unsupported custom tool call: exec", true},
		{"curr_time", "clock", "function_call_output", "unsupported call: clockcurr_time", true},
		{"exec", "functions", "function_call_output", "unsupported custom tool call: exec", false},
		{"exec", "functions", "custom_tool_call_output", "unrelated script error", false},
	} {
		t.Run(test.name+"/"+test.output, func(t *testing.T) {
			input := []any{map[string]any{"type": test.kind, "call_id": "call-1", "output": test.output}}
			request := map[string]any{"input": input}
			if integrationRegistryRejected(request, "call-1", test.name, test.namespace) != test.valid {
				t.Fatal("wrong registry classification")
			}
			if integrationRegistryRejected(request, "foreign-call", test.name, test.namespace) {
				t.Fatal("foreign call accepted")
			}
		})
	}
}

func (h *ephemeralIntegrationHarness) catalogControls(t *testing.T) {
	original := h.config
	h.write(t, filepath.Join(h.home, "config.toml"), "model_catalog_json="+strconv.Quote(h.catalog)+"\n"+original, 0600)
	// This dynamic daemon predates the user path: matching bytes/path do not
	// attest a startup StaticModelsManager. No utility thread may be created.
	h.mu.Lock()
	before := len(h.threadIDs)
	h.mu.Unlock()
	t.Run("dynamic user catalog rejected", func(t *testing.T) { h.run(t, "dynamic-catalog", "", "") })
	h.mu.Lock()
	after := len(h.threadIDs)
	h.mu.Unlock()
	if after != before {
		t.Fatal("dynamic catalog negative started a thread")
	}
	h.write(t, filepath.Join(h.home, "config.toml"), "model_catalog_json="+strconv.Quote(h.alternateCatalog)+"\n"+original, 0600)
	// CLI origin must still name the original canonical path, even when a user
	// layer points to another full unchanged catalog. Private preflight checks
	// includeLayers=false; each successful call depends on exact sessionFlags.
	t.Run("startup CLI catalog precedence", func(t *testing.T) { h.run(t, "baseline", "", "") })
	h.write(t, filepath.Join(h.home, "config.toml"), original, 0600)
}

func (h *ephemeralIntegrationHarness) reconfigure(t *testing.T, name string) {
	h.ordinary.Close()
	h.stop()
	h.config = strings.ReplaceAll(h.config, `mcp_servers."literal.name"`, `mcp_servers.`+strconv.Quote(name))
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
	h.start(t)
	// No existing call survives configuration mutation. Reopen identical client
	// options and read the retained control without starting ordinary writers.
	h.ordinary = h.ordinaryClient()
	h.retainedControl(t)
	h.verifyPersistence(t)
	if h.droppedPackets(t) != h.egressDrops {
		t.Fatal("reconfiguration caused denied egress")
	}
}
func (h *ephemeralIntegrationHarness) managedConflict(t *testing.T) {
	h.ordinary.Close()
	h.stop()
	h.write(t, "/etc/codex/requirements.toml", "[features]\nshell_tool = true\n", 0600)
	h.start(t)
	h.mu.Lock()
	h.requests = nil
	h.failure = ""
	h.mu.Unlock()
	client := New(h.utilitySocket)
	defer client.Close()
	opts := ephemeralTestOptions(t)
	opts.ModelCatalogFile = h.catalog
	_, err := runEphemeralTest(t, client, opts)
	if !errors.Is(err, ErrEphemeralIsolation) {
		t.Fatalf("managed restriction not refused: %v", err)
	}
	h.mu.Lock()
	count := len(h.requests)
	h.mu.Unlock()
	if count != 0 {
		t.Fatal("managed conflict reached inference")
	}
}

type ephemeralIntegrationSQLiteOwner struct {
	Table           string
	Prompt, OwnedID bool
	Statements      int
}

type ephemeralIntegrationSQLiteDiagnostic struct {
	File                                     string
	Prompt, OwnedID, UnknownOwner, Truncated bool
	Owners                                   []ephemeralIntegrationSQLiteOwner
}

// The complete current definition in pinned Codex 0.160.0
// state/logs_migrations/0002_logs_feedback_log_body.sql. This is a fixture
// acceptance boundary, not a schema compatibility or migration mechanism.
const integrationLogsDDL = `CREATE TABLE logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    ts_nanos INTEGER NOT NULL,
    level TEXT NOT NULL,
    target TEXT NOT NULL,
    feedback_log_body TEXT,
    module_path TEXT,
    file TEXT,
    line INTEGER,
    thread_id TEXT,
    process_uuid TEXT,
    estimated_bytes INTEGER NOT NULL DEFAULT 0
);`

// Scan the entire bounded dump. Quotes/comments keep embedded SQL text from
// becoming statements; incomplete quotes/comments or an unterminated tail fail.
// Reporting may cap its summaries, but acceptance never caps this walk.
func integrationSQLiteStatements(dump []byte, inspect func([]byte) error) error {
	if len(dump) > 4*1024*1024 {
		return errors.New("fixture SQLite dump exceeds bound")
	}
	start, quote := 0, byte(0)
	lineComment, blockComment := false, false
	for i := 0; i < len(dump); i++ {
		c := dump[i]
		if lineComment {
			if c == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if c == '*' && i+1 < len(dump) && dump[i+1] == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote != 0 {
			if c == quote {
				if quote != ']' && i+1 < len(dump) && dump[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(dump) && dump[i+1] == '-' {
			lineComment = true
			i++
			continue
		}
		if c == '/' && i+1 < len(dump) && dump[i+1] == '*' {
			blockComment = true
			i++
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		case ';':
			if err := inspect(dump[start : i+1]); err != nil {
				return err
			}
			start = i + 1
		}
	}
	if quote != 0 || blockComment || lineComment || len(bytes.TrimSpace(dump[start:])) != 0 {
		return errors.New("fixture SQLite dump is incomplete")
	}
	return nil
}

func integrationSQLiteSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

func integrationSQLiteWord(data []byte, word string) ([]byte, bool) {
	data = bytes.TrimSpace(data)
	if !bytes.HasPrefix(data, []byte(word)) || len(data) == len(word) || !integrationSQLiteSpace(data[len(word)]) {
		return nil, false
	}
	return bytes.TrimSpace(data[len(word):]), true
}

// Recognize only a complete dump-style row INSERT, not column lists, prefixes,
// near-match tables, qualified names, extra clauses or another SQL statement.
func integrationLogRow(statement []byte) bool {
	data, ok := integrationSQLiteWord(statement, "INSERT")
	if !ok {
		return false
	}
	data, ok = integrationSQLiteWord(data, "INTO")
	if !ok {
		return false
	}
	for _, table := range []string{"logs", `"logs"`} {
		rest, matches := integrationSQLiteWord(data, table)
		if !matches {
			continue
		}
		if !bytes.HasPrefix(rest, []byte("VALUES")) {
			return false
		}
		rest = bytes.TrimSpace(rest[len("VALUES"):])
		if len(rest) < 3 || rest[0] != '(' {
			return false
		}
		depth, quote := 0, byte(0)
		for i := 0; i < len(rest); i++ {
			c := rest[i]
			if quote != 0 {
				if c == quote {
					if quote != ']' && i+1 < len(rest) && rest[i+1] == quote {
						i++
					} else {
						quote = 0
					}
				}
				continue
			}
			if c == '-' && i+1 < len(rest) && rest[i+1] == '-' || c == '/' && i+1 < len(rest) && rest[i+1] == '*' {
				return false
			}
			switch c {
			case '\'', '"', '`':
				quote = c
			case '[':
				quote = ']'
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return bytes.Equal(bytes.TrimSpace(rest[i+1:]), []byte(";"))
				}
			}
		}
		return false
	}
	return false
}

func integrationLogPersistenceView(relative string, dump []byte) ([]byte, error) {
	if len(dump) > 4*1024*1024 {
		return nil, errors.New("fixture SQLite dump exceeds bound")
	}
	if relative != "logs_2.sqlite" {
		return dump, nil
	}
	var view bytes.Buffer
	started, completed, definitions := false, false, 0
	expected := strings.Join(strings.Fields(integrationLogsDDL), " ")
	err := integrationSQLiteStatements(dump, func(statement []byte) error {
		normalized := strings.Join(strings.Fields(string(statement)), " ")
		if completed {
			return errors.New("fixture diagnostic dump extends past COMMIT")
		}
		if normalized == "BEGIN TRANSACTION;" {
			if started {
				return errors.New("fixture diagnostic dump has repeated BEGIN")
			}
			started = true
		} else if !started {
			if normalized != "PRAGMA foreign_keys=OFF;" {
				return errors.New("fixture diagnostic dump lacks transaction")
			}
		} else if normalized == "COMMIT;" {
			completed = true
		}
		// Exact table token, including the dump's quoted spelling. An optional
		// IF NOT EXISTS is detected as an unsupported/ambiguous definition too.
		definition, create := integrationSQLiteWord(bytes.TrimSpace(statement), "CREATE")
		if create {
			definition, create = integrationSQLiteWord(definition, "TABLE")
		}
		if create {
			if optional, ok := integrationSQLiteWord(definition, "IF"); ok {
				if optional, ok = integrationSQLiteWord(optional, "NOT"); ok {
					if optional, ok = integrationSQLiteWord(optional, "EXISTS"); ok {
						definition = optional
					}
				}
			}
			for _, table := range []string{"logs", `"logs"`} {
				body := definition
				if !bytes.HasPrefix(body, []byte(table)) {
					continue
				}
				body = body[len(table):]
				if len(body) == 0 || !(integrationSQLiteSpace(body[0]) || body[0] == '(') {
					continue
				}
				definitions++
				canonical := strings.Replace(normalized, `CREATE TABLE "logs"`, "CREATE TABLE logs", 1)
				if definitions != 1 || canonical != expected {
					return errors.New("unsupported diagnostic log schema")
				}
			}
		}
		if integrationLogRow(statement) {
			if definitions != 1 || !started || completed {
				return errors.New("diagnostic log row preceded current schema")
			}
		} else {
			view.Write(statement)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !started || !completed || definitions != 1 {
		return nil, errors.New("fixture diagnostic dump lacks complete current schema")
	}
	return view.Bytes(), nil
}

func integrationPersistenceMarker(data []byte, ids []string) string {
	if bytes.Contains(data, []byte("RAW_UTILITY_PREFIX_SENTINEL")) {
		return "prompt"
	}
	for _, id := range ids {
		if id != "" && bytes.Contains(data, []byte(id)) {
			return "owned-id"
		}
	}
	return ""
}

func integrationSQLiteDiagnostic(relative string, dump []byte, ids []string) string {
	result := ephemeralIntegrationSQLiteDiagnostic{File: "[invalid]"}
	parts := strings.Split(relative, "/")
	valid := len(relative) <= 128 && len(parts) <= 4 && strings.HasSuffix(relative, ".sqlite")
	for _, part := range parts {
		valid = valid && integrationDiagnosticIdentifier(part) != "[invalid]"
	}
	if valid {
		result.File = relative
	}
	if len(dump) > 4*1024*1024 {
		return `{"File":"[invalid]","Truncated":true}`
	}
	if len(ids) > 64 {
		ids, result.Truncated = ids[:64], true
	}
	// Inspect only transient slices of the already bounded dump. SQL quoting
	// prevents a multiline row containing a fake INSERT from claiming ownership.
	inspect := func(statement []byte) {
		prompt, owned := bytes.Contains(statement, []byte("RAW_UTILITY_PREFIX_SENTINEL")), false
		for _, id := range ids {
			if len(id) > 128 {
				result.Truncated = true
			} else if id != "" && bytes.Contains(statement, []byte(id)) {
				owned = true
			}
		}
		if !prompt && !owned {
			return
		}
		result.Prompt, result.OwnedID = result.Prompt || prompt, result.OwnedID || owned
		name := "[unknown]"
		statement = bytes.TrimSpace(statement)
		if bytes.HasPrefix(statement, []byte("INSERT INTO ")) {
			token := bytes.TrimSpace(statement[len("INSERT INTO "):])
			if len(token) > 0 {
				end, start := 0, 0
				closing := byte(0)
				if token[0] == '"' || token[0] == '`' || token[0] == '[' {
					closing, start = token[0], 1
					if closing == '[' {
						closing = ']'
					}
				}
				for end = start; end < len(token) && end-start <= 64; end++ {
					if closing != 0 && token[end] == closing || closing == 0 && (token[end] == '(' || token[end] == ' ' || token[end] == '\n') {
						name = integrationDiagnosticIdentifier(string(token[start:end]))
						if closing != 0 && (end+1 >= len(token) || !(token[end+1] == '(' || token[end+1] == ' ' || token[end+1] == '\n' || token[end+1] == '\t')) {
							name = "[invalid]"
						}
						break
					}
				}
			}
		}
		if name == "[unknown]" || name == "[invalid]" {
			result.UnknownOwner = true
		}
		for i := range result.Owners {
			if result.Owners[i].Table == name {
				owner := &result.Owners[i]
				owner.Prompt, owner.OwnedID = owner.Prompt || prompt, owner.OwnedID || owned
				if owner.Statements < 64 {
					owner.Statements++
				} else {
					result.Truncated = true
				}
				return
			}
		}
		if len(result.Owners) >= 16 {
			result.Truncated = true
			return
		}
		result.Owners = append(result.Owners, ephemeralIntegrationSQLiteOwner{Table: name, Prompt: prompt, OwnedID: owned, Statements: 1})
	}
	if integrationSQLiteStatements(dump, func(statement []byte) error { inspect(statement); return nil }) != nil {
		result.Truncated = true
	}
	output, err := json.Marshal(result)
	if err != nil || len(output) > 4096 {
		return `{"File":"[invalid]","Truncated":true}`
	}
	return string(output)
}

func TestEphemeralIntegrationSQLiteDiagnostics(t *testing.T) {
	t.Run("actual owner categories without rows", func(t *testing.T) {
		dump := []byte("BEGIN TRANSACTION;\nINSERT INTO \"logs\" VALUES('RAW_UTILITY_PREFIX_SENTINEL: PROMPT_SECRET','owned-id-secret','ROW_SECRET');\nINSERT INTO threads VALUES('owned-id-secret');\nCOMMIT;")
		output := integrationSQLiteDiagnostic("logs_2.sqlite", dump, []string{"owned-id-secret"})
		var got ephemeralIntegrationSQLiteDiagnostic
		if json.Unmarshal([]byte(output), &got) != nil || got.File != "logs_2.sqlite" || !got.Prompt || !got.OwnedID || got.UnknownOwner || len(got.Owners) != 2 || got.Owners[0].Table != "logs" || !got.Owners[0].Prompt || !got.Owners[0].OwnedID || got.Owners[1].Table != "threads" || got.Owners[1].Prompt || !got.Owners[1].OwnedID {
			t.Fatal("marker ownership not identified")
		}
		for _, secret := range []string{"RAW_UTILITY_PREFIX_SENTINEL", "PROMPT_SECRET", "owned-id-secret", "ROW_SECRET", "INSERT", "VALUES"} {
			if strings.Contains(output, secret) {
				t.Fatal("SQLite diagnostic disclosed row data")
			}
		}
	})
	t.Run("quoted multiline row cannot manufacture a table", func(t *testing.T) {
		dump := []byte("INSERT INTO logs VALUES('quoted ''value'';\nINSERT INTO fake_owner VALUES(RAW_UTILITY_PREFIX_SENTINEL);');\nINSERT INTO \"unsafe secret\" VALUES('owned-id-secret');\nINSERT INTO \"logs\"\"secret\" VALUES('owned-id-secret');\nSELECT 'RAW_UTILITY_PREFIX_SENTINEL';")
		output := integrationSQLiteDiagnostic("../PRIVATE_SECRET.sqlite", dump, []string{"owned-id-secret"})
		var got ephemeralIntegrationSQLiteDiagnostic
		if json.Unmarshal([]byte(output), &got) != nil || got.File != "[invalid]" || !got.UnknownOwner || len(got.Owners) != 3 || got.Owners[0].Table != "logs" || got.Owners[0].OwnedID || got.Owners[1].Table != "[invalid]" || got.Owners[2].Table != "[unknown]" {
			t.Fatal("unsafe or false SQLite owner retained")
		}
		for _, secret := range []string{"fake_owner", "unsafe secret", "PRIVATE_SECRET", "owned-id-secret", "RAW_UTILITY_PREFIX_SENTINEL", "quoted"} {
			if strings.Contains(output, secret) {
				t.Fatal("SQLite diagnostic leaked unsupported content")
			}
		}
	})
	t.Run("owner count row count and size limits", func(t *testing.T) {
		var dump strings.Builder
		for i := 0; i < 100; i++ {
			dump.WriteString("INSERT INTO logs VALUES('RAW_UTILITY_PREFIX_SENTINEL');")
		}
		for i := 0; i < 40; i++ {
			fmt.Fprintf(&dump, "INSERT INTO table_%d VALUES('owned-id-secret');", i)
		}
		output := integrationSQLiteDiagnostic("private/logs_2.sqlite", []byte(dump.String()), []string{"owned-id-secret"})
		var got ephemeralIntegrationSQLiteDiagnostic
		if len(output) > 4096 || json.Unmarshal([]byte(output), &got) != nil || !got.Truncated || len(got.Owners) != 16 || got.Owners[0].Statements != 64 {
			t.Fatal("SQLite diagnostic limits lost")
		}
		if strings.Contains(output, "owned-id-secret") || strings.Contains(output, "RAW_UTILITY_PREFIX_SENTINEL") {
			t.Fatal("bounded metadata exposed markers")
		}
		oversize := integrationSQLiteDiagnostic("logs_2.sqlite", make([]byte, 4*1024*1024+1), nil)
		if oversize != `{"File":"[invalid]","Truncated":true}` {
			t.Fatal("oversize dump was inspected")
		}
	})
}

func TestEphemeralIntegrationPersistenceAcceptance(t *testing.T) {
	makeDump := func(ddl, statements string) []byte {
		return []byte("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n" + ddl + "\n" + statements + "\nCOMMIT;\n")
	}
	row := func(table, body string) string {
		body = strings.ReplaceAll(body, "'", "''")
		return "INSERT INTO " + table + " VALUES(1,1,1,'INFO','synthetic','" + body + "',NULL,NULL,NULL,'owned-id-secret','synthetic-process',1);"
	}
	allowed := row("logs", "RAW_UTILITY_PREFIX_SENTINEL: local diagnostic memory goal")
	quoted := row(`"logs"`, "RAW_UTILITY_PREFIX_SENTINEL: quoted 'value';\nINSERT INTO memories VALUES('owned-id-secret');\nCREATE TABLE logs (fake); -- memory goal")
	metadata := "INSERT INTO _sqlx_migrations VALUES('ordinary metadata');\nINSERT INTO sqlite_sequence VALUES('logs',2);"
	for _, test := range []struct {
		name, relative        string
		dump                  []byte
		accept, schemaFailure bool
	}{
		{"known unquoted diagnostic row", "logs_2.sqlite", makeDump(integrationLogsDDL, allowed), true, false},
		{"quoted multiline fake SQL stays in row", "logs_2.sqlite", makeDump(integrationLogsDDL, quoted), true, false},
		{"quoted DDL and whitespace", "logs_2.sqlite", makeDump(strings.ReplaceAll(strings.Replace(integrationLogsDDL, "TABLE logs", `TABLE "logs"`, 1), "    ", "\t"), quoted), true, false},
		{"empty logger with current schema", "logs_2.sqlite", makeDump(integrationLogsDDL, ""), true, false},
		{"prompt in other table", "logs_2.sqlite", makeDump(integrationLogsDDL, allowed+"\nINSERT INTO threads VALUES('RAW_UTILITY_PREFIX_SENTINEL');"), false, false},
		{"owned ID in other table", "logs_2.sqlite", makeDump(integrationLogsDDL, allowed+"\nINSERT INTO threads VALUES('owned-id-secret');"), false, false},
		{"another database", "state_5.sqlite", makeDump(integrationLogsDDL, allowed), false, false},
		{"nested logger basename", "nested/logs_2.sqlite", makeDump(integrationLogsDDL, allowed), false, false},
		{"prompt in metadata", "logs_2.sqlite", makeDump(integrationLogsDDL, "INSERT INTO _sqlx_migrations VALUES('RAW_UTILITY_PREFIX_SENTINEL');"), false, false},
		{"owned ID in sequence", "logs_2.sqlite", makeDump(integrationLogsDDL, "INSERT INTO sqlite_sequence VALUES('owned-id-secret',1);"), false, false},
		{"marker in other DDL", "logs_2.sqlite", makeDump(integrationLogsDDL+"\nCREATE TABLE RAW_UTILITY_PREFIX_SENTINEL (value TEXT);", ""), false, false},
		{"marker in logger DDL", "logs_2.sqlite", makeDump(strings.Replace(integrationLogsDDL, "target TEXT", "RAW_UTILITY_PREFIX_SENTINEL TEXT", 1), ""), false, true},
		{"normal file prompt", "ordinary.txt", []byte("RAW_UTILITY_PREFIX_SENTINEL: retained file"), false, false},
		{"normal file owned ID", "ordinary.txt", []byte("owned-id-secret"), false, false},
		{"old message schema", "logs_2.sqlite", makeDump(strings.Replace(integrationLogsDDL, "feedback_log_body", "message", 1), allowed), false, true},
		{"missing schema", "logs_2.sqlite", makeDump("", ""), false, true},
		{"changed column type", "logs_2.sqlite", makeDump(strings.Replace(integrationLogsDDL, "line INTEGER", "line TEXT", 1), allowed), false, true},
		{"changed default", "logs_2.sqlite", makeDump(strings.Replace(integrationLogsDDL, "DEFAULT 0", "DEFAULT 1", 1), allowed), false, true},
		{"ambiguous duplicate schema", "logs_2.sqlite", makeDump(integrationLogsDDL+"\n"+integrationLogsDDL, allowed), false, true},
		{"ambiguous optional second schema", "logs_2.sqlite", makeDump(integrationLogsDDL+"\n"+strings.Replace(integrationLogsDDL, "CREATE TABLE logs", "CREATE TABLE IF\nNOT EXISTS logs", 1), allowed), false, true},
		{"near-match logs_extra", "logs_2.sqlite", makeDump(integrationLogsDDL, row("logs_extra", "RAW_UTILITY_PREFIX_SENTINEL")), false, false},
		{"near-match quoted logs name", "logs_2.sqlite", makeDump(integrationLogsDDL, row(`"logs""extra"`, "RAW_UTILITY_PREFIX_SENTINEL")), false, false},
		{"qualified table name", "logs_2.sqlite", makeDump(integrationLogsDDL, row("main.logs", "RAW_UTILITY_PREFIX_SENTINEL")), false, false},
		{"column list is not dump row", "logs_2.sqlite", makeDump(integrationLogsDDL, "INSERT INTO logs(feedback_log_body) VALUES('RAW_UTILITY_PREFIX_SENTINEL');"), false, false},
		{"extra clause is not dump row", "logs_2.sqlite", makeDump(integrationLogsDDL, strings.TrimSuffix(allowed, ";")+" RETURNING id;"), false, false},
		{"comment is not row data", "logs_2.sqlite", makeDump(integrationLogsDDL, "INSERT INTO logs VALUES(/* RAW_UTILITY_PREFIX_SENTINEL */1);"), false, false},
		{"missing COMMIT", "logs_2.sqlite", []byte("BEGIN TRANSACTION;\n" + integrationLogsDDL + "\n" + allowed), false, true},
		{"unterminated quoted row", "logs_2.sqlite", []byte("BEGIN TRANSACTION;\n" + integrationLogsDDL + "\nINSERT INTO logs VALUES('RAW_UTILITY_PREFIX_SENTINEL;\nCOMMIT;"), false, true},
		{"unterminated last statement", "logs_2.sqlite", []byte("BEGIN TRANSACTION;\n" + integrationLogsDDL + "\nCOMMIT"), false, true},
		{"unterminated comment", "logs_2.sqlite", []byte("BEGIN TRANSACTION;\n" + integrationLogsDDL + "\n/* COMMIT;"), false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			view, err := integrationLogPersistenceView(test.relative, test.dump)
			if (err != nil) != test.schemaFailure {
				t.Fatal("wrong schema/completeness result")
			}
			accepted := err == nil && integrationPersistenceMarker(view, []string{"owned-id-secret"}) == ""
			if accepted != test.accept {
				t.Fatal("incorrect persistence exception")
			}
			if test.relative != "logs_2.sqlite" && !bytes.Equal(view, test.dump) {
				t.Fatal("non-root content was filtered")
			}
		})
	}
	t.Run("all non-row statements and database inventory survive", func(t *testing.T) {
		index := "CREATE INDEX idx_logs_ts ON logs(ts DESC, ts_nanos DESC, id DESC);"
		other := "INSERT INTO unrelated VALUES('ordinary memory/goal metadata');"
		dump := makeDump(integrationLogsDDL, allowed+"\n"+index+"\n"+metadata+"\n"+other)
		view, err := integrationLogPersistenceView("logs_2.sqlite", dump)
		if err != nil || integrationPersistenceMarker(view, []string{"owned-id-secret"}) != "" {
			t.Fatal("known diagnostic row not accepted")
		}
		for _, statement := range []string{integrationLogsDDL, index, metadata, other, "PRAGMA foreign_keys=OFF;", "BEGIN TRANSACTION;", "COMMIT;"} {
			if !bytes.Contains(view, []byte(statement)) {
				t.Fatal("non-row statement discarded")
			}
		}
		if bytes.Contains(view, []byte("INSERT INTO logs VALUES")) {
			t.Fatal("logger row left in view")
		}
		// snapshot still inserts this database into its original files map even
		// when there are no memory/goal lines; the view never deletes its file.
	})
	t.Run("late marker after more than64 allowed rows", func(t *testing.T) {
		statements := strings.Repeat(allowed+"\n", 70) + "INSERT INTO threads VALUES('RAW_UTILITY_PREFIX_SENTINEL');"
		view, err := integrationLogPersistenceView("logs_2.sqlite", makeDump(integrationLogsDDL, statements))
		if err != nil || integrationPersistenceMarker(view, nil) != "prompt" {
			t.Fatal("reporting row cap leaked into acceptance")
		}
	})
	t.Run("all owned IDs exceed diagnostic cap", func(t *testing.T) {
		ids := make([]string, 70)
		for i := range ids {
			ids[i] = fmt.Sprintf("utility-owned-id-%03d", i)
		}
		statements := strings.Repeat(allowed+"\n", 70) + "INSERT INTO threads VALUES('" + ids[69] + "');"
		view, err := integrationLogPersistenceView("logs_2.sqlite", makeDump(integrationLogsDDL, statements))
		if err != nil || integrationPersistenceMarker(view, ids) != "owned-id" {
			t.Fatal("reporting ID cap leaked into acceptance")
		}
	})
	for _, seed := range []struct{ name, row string }{
		{"goals_1.sqlite", "INSERT INTO thread_goals VALUES('control-root','goal-id','GOAL_STATE_SENTINEL','paused',1,1);"},
		{"memories_1.sqlite", "INSERT INTO stage1_outputs VALUES('memory-root',1,'MEMORY_POLICY_SENTINEL','synthetic retained memory',1);"},
	} {
		t.Run("whole seeded dump retained "+seed.name, func(t *testing.T) {
			dump := makeDump("", seed.row)
			view, err := integrationLogPersistenceView(seed.name, dump)
			if err != nil || !bytes.Equal(view, dump) {
				t.Fatal("seeded sensitive dump normalized")
			}
			changed := bytes.Replace(dump, []byte("SENTINEL"), []byte("CHANGED"), 1)
			after, err := integrationLogPersistenceView(seed.name, changed)
			if err != nil || bytes.Equal(after, view) {
				t.Fatal("seeded byte change erased")
			}
		})
	}
	t.Run("inspection bound is not a summary cap", func(t *testing.T) {
		if _, err := integrationLogPersistenceView("logs_2.sqlite", make([]byte, 4*1024*1024+1)); err == nil {
			t.Fatal("oversized dump accepted")
		}
	})
}

func (h *ephemeralIntegrationHarness) snapshot(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(h.home, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(h.home, path)
		if strings.HasSuffix(relative, "-wal") || strings.HasSuffix(relative, "-shm") {
			return nil
		}
		if strings.HasSuffix(relative, ".sqlite") {
			sqlite := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
			// The dump is synthetic fixture state only and retained in bounded memory.
			output, err := exec.Command(sqlite, path, ".dump").Output()
			if err != nil || len(output) > 4*1024*1024 {
				return errors.New("fixture SQLite inspection failed")
			}
			h.mu.Lock()
			ids := append([]string(nil), h.threadIDs...)
			h.mu.Unlock()
			view, viewErr := integrationLogPersistenceView(relative, output)
			if viewErr != nil {
				t.Logf("native SQLite marker metadata: %s", integrationSQLiteDiagnostic(relative, output, ids))
				return viewErr
			}
			if marker := integrationPersistenceMarker(view, ids); marker != "" {
				t.Logf("native SQLite marker metadata: %s", integrationSQLiteDiagnostic(relative, view, ids))
				if marker == "prompt" {
					return errors.New("utility prompt persisted in SQLite")
				}
				return errors.New("ephemeral identity persisted in SQLite")
			}
			// Goal/memory tables must remain byte-identical; ordinary metadata may
			// change counters/time without carrying any utility prompt or identity.
			if strings.HasPrefix(filepath.Base(relative), "goals_") || strings.HasPrefix(filepath.Base(relative), "memories_") {
				files[relative] = string(view)
				return nil
			}
			var sensitive []string
			for _, line := range strings.Split(string(view), "\n") {
				lower := strings.ToLower(line)
				if strings.Contains(lower, "memory") || strings.Contains(lower, "goal") {
					sensitive = append(sensitive, line)
				}
			}
			files[relative] = strings.Join(sensitive, "\n")
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected persistent symlink")
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) > 4*1024*1024 {
			return errors.New("fixture persistent-file inspection failed")
		}
		h.mu.Lock()
		ids := append([]string(nil), h.threadIDs...)
		h.mu.Unlock()
		if marker := integrationPersistenceMarker(data, ids); marker != "" {
			if marker == "prompt" {
				return errors.New("utility prompt persisted in a file")
			}
			return errors.New("ephemeral thread/rollout persisted")
		}
		for _, id := range ids {
			if id != "" && strings.Contains(relative, id) {
				return errors.New("ephemeral thread/rollout persisted")
			}
		}
		if relative == "models_cache.json" {
			return nil
		} // inventoried ordinary catalog cache
		sum := sha256.Sum256(data)
		files[relative] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
func (h *ephemeralIntegrationHarness) verifyPersistence(t *testing.T) {
	current := h.snapshot(t)
	for path, before := range h.baseline {
		if path == "config.toml" {
			continue
		} // operator change occurs between stopped calls
		if current[path] != before {
			t.Fatalf("persistent state changed: %s", path)
		}
	}
	for path := range current {
		if _, known := h.baseline[path]; !known {
			t.Fatalf("unexpected persistent file: %s", path)
		}
	}
}

func (h *ephemeralIntegrationHarness) droppedPackets(t *testing.T) uint64 {
	packets, err := h.readDroppedPackets()
	if err != nil {
		t.Fatal("cannot inspect real egress counters")
	}
	return packets
}

func (h *ephemeralIntegrationHarness) readDroppedPackets() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	output, err := exec.CommandContext(ctx, h.nft, "-j", "list", "table", "inet", "ephemeral_fixture").Output()
	if err != nil || len(output) > 128*1024 {
		return 0, errors.New("egress counter unavailable")
	}
	var object any
	if json.Unmarshal(output, &object) != nil {
		return 0, errors.New("malformed egress counters")
	}
	var packets uint64
	var walk func(any)
	walk = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			if counter, ok := node["counter"].(map[string]any); ok {
				if count, ok := counter["packets"].(float64); ok {
					packets += uint64(count)
				}
			}
			for _, value := range node {
				walk(value)
			}
		case []any:
			for _, value := range node {
				walk(value)
			}
		}
	}
	walk(object)
	return packets, nil
}
