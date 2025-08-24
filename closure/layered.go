package closure

import (
	"github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"sort"
)

func buildGraph(storepaths []Storepath) (map[string]int64, *simple.DirectedGraph) {
	g := simple.NewDirectedGraph()
	paths := make(map[string]int64)
	for _, p := range storepaths {
		var u, v graph.Node
		if id, ok := paths[p.Path]; ok {
			u = g.Node(id)
		} else {
			u = g.NewNode()
			g.AddNode(u)
			paths[p.Path] = u.ID()
		}
		for _, r := range p.References {
			if id, ok := paths[r]; ok {
				v = g.Node(id)
			} else {
				v = g.NewNode()
				g.AddNode(v)
				paths[r] = v.ID()
			}
			if u == v {
				continue
			}
			g.SetEdge(g.NewEdge(u, v))
		}
	}
	return paths, g
}

// SortedPathsByPopularity sorts storepaths by path popularity. It uses the algorithm described in
// https://grahamc.com/blog/nix-and-layered-docker-images
func SortedPathsByPopularity(storepaths []Storepath) ([]string, error) {
	paths, g := buildGraph(storepaths)
	scored, err := Score(g)
	if err != nil {
		return []string{}, err
	}

	out := make([]string, len(scored))
	for i, s := range scored {
		for p, id := range paths {
			if id == s.id {
				logrus.Debugf("Score: %d (%s)", s.score, p)
				out[i] = p
			}
		}
	}
	return out, nil
}

// SortedPathsByNarSize sorts storepaths by descending NAR size.
// If two paths have the same size, they are ordered lexicographically by path
// to keep the ordering deterministic.
func SortedPathsByNarSize(storepaths []Storepath) ([]string, error) {
    type sizedPath struct {
        path string
        size int64
    }
    sized := make([]sizedPath, 0, len(storepaths))
    for _, sp := range storepaths {
        sized = append(sized, sizedPath{path: sp.Path, size: sp.NarSize})
    }
    sort.Slice(sized, func(i, j int) bool {
        if sized[i].size == sized[j].size {
            return sized[i].path < sized[j].path
        }
        return sized[i].size > sized[j].size
    })
    out := make([]string, len(sized))
    for i, s := range sized {
        out[i] = s.path
    }
    return out, nil
}
