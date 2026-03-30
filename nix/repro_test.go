package nix

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nlewo/nix2container/types"
	digest "github.com/opencontainers/go-digest"
)

// =============================================================================
// Core repro: symlink <-> regular-file substitution
//
// Reproduces the "Digest did not match" error from nix2container#127.
// Root cause: NixOS/nix#10525 -- the Nix build sandbox resolves symlinks
// to regular files. The digest computed at build time (inside sandbox)
// differs from the tar generated at push time (outside sandbox).
//
// The old code called TarPathsSum once (for the manifest digest) then
// TarPaths again (for GetBlob). Any filesystem change between these two
// calls causes the containers/image digest verifier to fail.
// =============================================================================

// TestSymlinkToFileSubstitution shows symlink->file changes the digest.
func TestSymlinkToFileSubstitution(t *testing.T) {
	root := t.TempDir()
	populateWithSymlinks(t, root)
	paths := types.Paths{{Path: root, Options: &types.PathOptions{}}}

	dSymlinks, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("TarPathsSum with symlinks: %v", err)
	}
	t.Logf("digest with symlinks:      %s", dSymlinks)

	replaceSymlinksWithFiles(t, root)

	dFiles, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("TarPathsSum with files: %v", err)
	}
	t.Logf("digest with regular files: %s", dFiles)

	if dSymlinks == dFiles {
		t.Errorf("expected different digests, got same: %s", dSymlinks)
	} else {
		t.Logf("CONFIRMED: symlink->file substitution changes the digest")
	}
}

// TestProductionFlowMismatch reproduces the exact production failure:
// 1. resolveLayerDigests calls TarPathsSum (digest goes into manifest)
// 2. filesystem changes (concurrent nix operations / sandbox bug)
// 3. GetBlob calls TarPaths (bytes streamed to ECR)
// 4. containers/image verifier detects digest != bytes
func TestProductionFlowMismatch(t *testing.T) {
	root := t.TempDir()
	populateWithSymlinks(t, root)
	paths := types.Paths{{Path: root, Options: &types.PathOptions{}}}

	manifestDigest, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("TarPathsSum (resolve): %v", err)
	}
	t.Logf("manifest digest (resolveLayerDigests): %s", manifestDigest)

	replaceSymlinksWithFiles(t, root)

	rd := TarPaths(paths)
	h := sha256.New()
	io.Copy(h, rd)
	rd.Close()
	blobDigest := fmt.Sprintf("sha256:%x", h.Sum(nil))
	t.Logf("blob digest    (GetBlob/TarPaths):     %s", blobDigest)

	if manifestDigest.String() == blobDigest {
		t.Errorf("expected mismatch but digests matched: %s", blobDigest)
	} else {
		t.Logf("REPRODUCED: Digest did not match, expected %s, got %s", manifestDigest, blobDigest)
	}
}

// TestMaterializePreventsMismatch proves MaterializeReproducibleLayers
// eliminates the mismatch even when the filesystem changes.
func TestMaterializePreventsMismatch(t *testing.T) {
	root := t.TempDir()
	populateWithSymlinks(t, root)

	image := types.Image{Layers: []types.Layer{{
		Digest:  "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		DiffIDs: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		Paths:   types.Paths{{Path: root, Options: &types.PathOptions{}}},
	}}}

	tmpDir, err := MaterializeReproducibleLayers(&image)
	if err != nil {
		t.Fatal(err)
	}
	if tmpDir != "" {
		defer os.RemoveAll(tmpDir)
	}
	materializedDigest := image.Layers[0].Digest
	layerPath := image.Layers[0].LayerPath
	t.Logf("materialized digest: %s", materializedDigest)

	replaceSymlinksWithFiles(t, root)

	f, err := os.Open(layerPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	digester := digest.Canonical.Digester()
	io.Copy(digester.Hash(), f)
	blobDigest := digester.Digest()

	if blobDigest.String() != materializedDigest {
		t.Errorf("materialized blob digest changed: %s vs %s", blobDigest, materializedDigest)
	} else {
		t.Logf("CONFIRMED: materialized layer immune to filesystem changes (digest=%s)", blobDigest)
	}
}

// TestFileToSymlinkSubstitution tests the reverse direction.
func TestFileToSymlinkSubstitution(t *testing.T) {
	root := t.TempDir()
	populateWithSymlinks(t, root)
	replaceSymlinksWithFiles(t, root)
	paths := types.Paths{{Path: root, Options: &types.PathOptions{}}}

	dBuild, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("build-time TarPathsSum: %v", err)
	}
	t.Logf("build-time digest (sandbox):   %s", dBuild)

	restoreSymlinks(t, root)

	dPush, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("push-time TarPathsSum: %v", err)
	}
	t.Logf("push-time digest (real):       %s", dPush)

	if dBuild == dPush {
		t.Errorf("expected different digests, got same: %s", dBuild)
	} else {
		t.Logf("CONFIRMED: sandbox->real transition changes digest")
	}
}

// =============================================================================
// Determinism / stress tests
// =============================================================================

func TestTarPathsDeterminism(t *testing.T) {
	root := t.TempDir()
	populate(t, root)
	paths := types.Paths{{Path: root, Options: &types.PathOptions{}}}

	baseline, _, err := TarPathsSum(paths)
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	t.Logf("baseline: %s", baseline)

	for i := 0; i < 50; i++ {
		d, _, _ := TarPathsSum(paths)
		if d != baseline {
			t.Fatalf("SERIAL MISMATCH iter %d: %s vs %s", i, d, baseline)
		}
	}
	t.Logf("serial: 50 ok")

	const C, R = 32, 200
	var wg sync.WaitGroup
	var mismatches int64
	for r := 0; r < R; r++ {
		for g := 0; g < C; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, _, _ := TarPathsSum(paths)
				if d != baseline {
					atomic.AddInt64(&mismatches, 1)
				}
			}()
		}
	}
	wg.Wait()
	if m := atomic.LoadInt64(&mismatches); m > 0 {
		t.Errorf("concurrent: %d/%d mismatches", m, C*R)
	} else {
		t.Logf("concurrent: %d calls all matched", C*R)
	}
}

func TestNixStorePaths(t *testing.T) {
	entries, err := os.ReadDir("/nix/store")
	if err != nil {
		t.Skip("no /nix/store")
	}
	var dirs []string
	for _, e := range entries {
		if len(dirs) >= 5 {
			break
		}
		p := filepath.Join("/nix/store", e.Name())
		if fi, err := os.Lstat(p); err == nil && fi.IsDir() {
			dirs = append(dirs, p)
		}
	}
	if len(dirs) == 0 {
		t.Skip("no store dirs")
	}
	for _, d := range dirs {
		d := d
		name := filepath.Base(d)
		if len(name) > 40 {
			name = name[:40]
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			paths := types.Paths{{Path: d, Options: &types.PathOptions{}}}
			baseline, _, err := TarPathsSum(paths)
			if err != nil {
				t.Skipf("skip: %v", err)
			}
			var mismatches int64
			var wg sync.WaitGroup
			for i := 0; i < 200; i++ {
				for g := 0; g < 16; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						d2, _, err := TarPathsSum(paths)
						if err != nil {
							return
						}
						if d2 != baseline {
							atomic.AddInt64(&mismatches, 1)
						}
					}()
				}
			}
			wg.Wait()
			if m := atomic.LoadInt64(&mismatches); m > 0 {
				t.Errorf("MISMATCH %d/3200 on %s", m, d)
			} else {
				t.Logf("3200 calls matched: %s", baseline)
			}
		})
	}
}

func TestLongRunningStress(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	root := t.TempDir()
	populateLarge(t, root, 1000)
	paths := types.Paths{{Path: root, Options: &types.PathOptions{}}}
	baseline, _, _ := TarPathsSum(paths)

	dur := 30 * time.Second
	t.Logf("stress %v, baseline %s", dur, baseline)
	var total, mismatches int64
	done := make(chan struct{})
	go func() { time.Sleep(dur); close(done) }()

	var wg sync.WaitGroup
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				d, _, err := TarPathsSum(paths)
				atomic.AddInt64(&total, 1)
				if err == nil && d != baseline {
					atomic.AddInt64(&mismatches, 1)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("%d calls, %d mismatches", total, mismatches)
}

// =============================================================================
// Helpers
// =============================================================================

func populateWithSymlinks(t *testing.T, root string) {
	t.Helper()
	for _, d := range []string{"bin", "lib", "lib/python3.11", "lib/python3.11/site-packages", "share", "share/man", "share/man/man1", "etc"} {
		os.MkdirAll(filepath.Join(root, d), 0755)
	}
	files := map[string]int{
		"bin/python3.11": 8192, "lib/libpython3.11.so.1.0": 65536,
		"lib/python3.11/os.py": 4096, "lib/python3.11/site-packages/pip": 2048,
		"share/man/man1/python3.11.1": 1024, "etc/config.json": 512,
	}
	for n, sz := range files {
		data := make([]byte, sz)
		for i := range data {
			data[i] = byte(len(n) + i%251)
		}
		os.WriteFile(filepath.Join(root, n), data, 0644)
	}
	os.Chmod(filepath.Join(root, "bin/python3.11"), 0755)
	for name, target := range map[string]string{
		"bin/python3": "python3.11", "bin/python": "python3",
		"lib/libpython3.11.so": "libpython3.11.so.1.0", "lib/libpython3.so": "libpython3.11.so",
	} {
		os.Symlink(target, filepath.Join(root, name))
	}
}

func replaceSymlinksWithFiles(t *testing.T, root string) {
	t.Helper()
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			return err
		}
		target, _ := os.Readlink(path)
		dir := filepath.Dir(path)
		resolved := filepath.Join(dir, target)
		var content []byte
		if ri, e := os.Lstat(resolved); e == nil && ri.Mode().IsRegular() {
			content, _ = os.ReadFile(resolved)
		} else {
			content = []byte(target)
		}
		os.Remove(path)
		return os.WriteFile(path, content, 0644)
	})
}

func restoreSymlinks(t *testing.T, root string) {
	t.Helper()
	for name, target := range map[string]string{
		"bin/python3": "python3.11", "bin/python": "python3",
		"lib/libpython3.11.so": "libpython3.11.so.1.0", "lib/libpython3.so": "libpython3.11.so",
	} {
		p := filepath.Join(root, name)
		os.Remove(p)
		os.Symlink(target, p)
	}
}

func populate(t *testing.T, root string) {
	t.Helper()
	for _, d := range []string{"bin", "lib", "lib/python3.11", "share", "etc"} {
		os.MkdirAll(filepath.Join(root, d), 0755)
	}
	for n, sz := range map[string]int{"bin/python": 8192, "lib/libpython.so": 65536, "lib/python3.11/mod.py": 4096, "share/doc.1": 2048, "etc/cfg.json": 512} {
		data := make([]byte, sz)
		for i := range data {
			data[i] = byte(len(n) + i%251)
		}
		os.WriteFile(filepath.Join(root, n), data, 0644)
	}
	os.Chmod(filepath.Join(root, "bin/python"), 0755)
	os.Symlink("libpython.so", filepath.Join(root, "lib/libpython3.so"))
	os.Symlink("python", filepath.Join(root, "bin/python3"))
}

func populateLarge(t *testing.T, root string, n int) {
	t.Helper()
	dirs := []string{"bin", "lib", "share", "etc", "var", "opt"}
	for _, d := range dirs {
		os.MkdirAll(filepath.Join(root, d), 0755)
	}
	for i := 0; i < n; i++ {
		data := make([]byte, 512+(i%10)*1024)
		for j := range data {
			data[j] = byte(i*7 + j%251)
		}
		os.WriteFile(filepath.Join(root, dirs[i%len(dirs)], fmt.Sprintf("f%04d", i)), data, 0644)
	}
}

func populateDeep(t *testing.T, root string, depth int) {
	t.Helper()
	cur := root
	for i := 0; i < depth; i++ {
		cur = filepath.Join(cur, fmt.Sprintf("d%d", i))
		os.MkdirAll(cur, 0755)
		os.WriteFile(filepath.Join(cur, fmt.Sprintf("f%d", i)),
			[]byte(strings.Repeat("x", 1024)), 0644)
	}
}
