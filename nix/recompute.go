package nix

import (
	"io"

	"github.com/nlewo/nix2container/types"
	godigest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// RecomputeLayerDigests reads each layer blob via LayerGetBlob and
// recomputes its digest and size from the actual bytes that will be
// served at push time. This prevents "Digest did not match" errors
// when the pre-computed digest in the image JSON differs from the
// bytes produced by LayerGetBlob (e.g. because nix2container-bin and
// skopeo-nix2container were built with different Go toolchains whose
// archive/tar output diverges, or because the layer tar was
// regenerated on a machine with different nix store contents).
func RecomputeLayerDigests(image *types.Image) error {
	for i, layer := range image.Layers {
		rc, _, err := LayerGetBlob(layer)
		if err != nil {
			return err
		}
		digester := godigest.Canonical.Digester()
		size, err := io.Copy(digester.Hash(), rc)
		rc.Close()
		if err != nil {
			return err
		}
		actual := digester.Digest().String()
		if actual != layer.Digest {
			logrus.Infof("layer %d: recomputed digest %s (was %s, size %d->%d)", i, actual, layer.Digest, layer.Size, size)
			image.Layers[i].Digest = actual
			image.Layers[i].DiffIDs = actual
			image.Layers[i].Size = size
		}
	}
	return nil
}
