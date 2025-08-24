package nix

import (
	_ "crypto/sha256"
	_ "crypto/sha512"
	"reflect"
	"runtime"
	"strconv"
	"sync"

	"github.com/nlewo/nix2container/types"
	godigest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

func getPaths(storePaths []string, parents []types.Layer, rewrites []types.RewritePath, exclude string, permPaths []types.PermPath) types.Paths {
	var paths types.Paths
	for _, p := range storePaths {
		path := types.Path{
			Path: p,
		}
		var pathOptions types.PathOptions
		hasPathOptions := false
		var perms []types.Perm
		for _, perm := range permPaths {
			if p == perm.Path {
				hasPathOptions = true
				perms = append(perms, types.Perm{
					Regex: perm.Regex,
					Mode:  perm.Mode,
					Uid:   perm.Uid,
					Gid:   perm.Gid,
					Uname: perm.Uname,
					Gname: perm.Gname,
				})
			}
		}
		if perms != nil {
			pathOptions.Perms = perms
		}
		for _, rewrite := range rewrites {
			if p == rewrite.Path {
				hasPathOptions = true
				pathOptions.Rewrite = types.Rewrite{
					Regex: rewrite.Regex,
					Repl:  rewrite.Repl,
				}
			}
		}
		if hasPathOptions {
			path.Options = &pathOptions
		}
		if p == exclude {
			logrus.Infof("Excluding path %s from layer", p)
			continue
		}
		if isPathInLayers(parents, path) {
			logrus.Infof("Excluding path %s because already present in a parent layer", p)
			continue
		}
		paths = append(paths, path)
	}
	return paths
}

// If tarDirectory is not an empty string, the tar layer is written to
// the disk. This is useful for layer containing non reproducible
// store paths.
func newLayers(paths types.Paths, tarDirectory string, maxLayers int, history v1.History) (layers []types.Layer, err error) {
	totalPaths := len(paths)
	if totalPaths == 0 {
		return []types.Layer{}, nil
	}
	if maxLayers < 1 {
		maxLayers = 1
	}
	numLayers := maxLayers
	if totalPaths < maxLayers {
		numLayers = totalPaths
	}

	type layerJob struct {
		index      int
		layerPaths types.Paths
	}
	jobs := make([]layerJob, 0, numLayers)
	for i := 0; i < numLayers; i++ {
		end := i + 1
		if i == numLayers-1 {
			end = totalPaths
		}
		jobs = append(jobs, layerJob{index: i, layerPaths: paths[i:end]})
	}

	workers := 0
	if v := "64"; v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n > 0 {
			workers = n
		}
	}
	if workers <= 0 {
		p := runtime.GOMAXPROCS(0)
		if p > 16 {
			p = 16
		}
		if p > numLayers {
			p = numLayers
		}
		if p < 1 {
			p = 1
		}
		workers = p
	}

	type result struct {
		index int
		layer types.Layer
		err   error
	}

	jobCh := make(chan layerJob)
	resCh := make(chan result, numLayers)
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for j := range jobCh {
				var (
					d  godigest.Digest
					sz int64
					lp string
					e  error
				)
				if tarDirectory == "" {
					d, sz, e = TarPathsSum(j.layerPaths)
				} else {
					lp, d, sz, e = TarPathsWrite(j.layerPaths, tarDirectory)
				}
				if e != nil {
					resCh <- result{index: j.index, err: e}
					continue
				}

				logrus.Infof("Adding %d paths to layer (size:%d digest:%s)", len(j.layerPaths), sz, d.String())
				lay := types.Layer{
					Digest:    d.String(),
					DiffIDs:   d.String(),
					Size:      sz,
					Paths:     j.layerPaths,
					MediaType: v1.MediaTypeImageLayer,
					History:   history,
				}
				if tarDirectory != "" {
					// TODO: we should use v1.MediaTypeImageLayerGzip instead
					lay.MediaType = v1.MediaTypeImageLayer
					lay.LayerPath = lp
				}
				resCh <- result{index: j.index, layer: lay, err: nil}
			}
		}()
	}

	go func() {
		for _, j := range jobs {
			jobCh <- j
		}
		close(jobCh)
	}()

	layersByIndex := make([]types.Layer, numLayers)
	var firstErr error
	for i := 0; i < numLayers; i++ {
		r := <-resCh
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		layersByIndex[r.index] = r.layer
	}
	wg.Wait()
	close(resCh)
	if firstErr != nil {
		return layers, firstErr
	}

	layers = append(layers, layersByIndex...)
	return layers, nil
}

func NewLayers(storePaths []string, maxLayers int, parents []types.Layer, rewrites []types.RewritePath, exclude string, perms []types.PermPath, history v1.History) ([]types.Layer, error) {
	paths := getPaths(storePaths, parents, rewrites, exclude, perms)
	return newLayers(paths, "", maxLayers, history)
}

func NewLayersNonReproducible(storePaths []string, maxLayers int, tarDirectory string, parents []types.Layer, rewrites []types.RewritePath, exclude string, perms []types.PermPath, history v1.History) (layers []types.Layer, err error) {
	paths := getPaths(storePaths, parents, rewrites, exclude, perms)
	return newLayers(paths, tarDirectory, maxLayers, history)
}

func isPathInLayers(layers []types.Layer, path types.Path) bool {
	for _, layer := range layers {
		for _, p := range layer.Paths {
			if reflect.DeepEqual(p, path) {
				return true
			}
		}
	}
	return false
}
