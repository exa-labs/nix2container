// This package can be used to generate image configuration and blobs
// from an image JSON file.
//
// First, you need to create an types.Image with NewImageFromFile.
//
// With this types.Image object, it is then possible to get the image
// configuration with the GetConfigBlob method. To get layer blobs,
// you need to iterate on the layers of the image and use the GetBlob
// or LayerGetBlob functions to get a Reader on this layer.
package nix

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/containers/image/v5/manifest"
	"github.com/nlewo/nix2container/types"
	godigest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

// GetConfigBlob returns the config blog of an image.
func GetConfigBlob(image types.Image) ([]byte, error) {
	imageV1, err := getV1Image(image)
	if err != nil {
		return nil, err
	}
	configBlob, err := json.Marshal(imageV1)
	if err != nil {
		return nil, err
	}
	return configBlob, nil
}

// GetConfigDigest returns the digest and the size of the config blog of an image.
func GetConfigDigest(image types.Image) (d godigest.Digest, size int64, err error) {
	configBlob, err := GetConfigBlob(image)
	if err != nil {
		return d, size, err
	}
	d = godigest.FromBytes(configBlob)
	return d, int64(len(configBlob)), err
}

// GetBlob gets the layer corresponding to the provided digest.
func GetBlob(image types.Image, digest godigest.Digest) (io.ReadCloser, int64, error) {
	for _, layer := range image.Layers {
		if layer.Digest == digest.String() {
			return LayerGetBlob(layer)
		}
	}
	configDigest, _, err := GetConfigDigest(image)
	if err != nil {
		return nil, 0, err
	}
	if digest == configDigest {
		configBlob, err := GetConfigBlob(image)
		if err != nil {
			return nil, 0, err
		}
		rc := nopCloser{bytes.NewReader(configBlob)}
		return rc, int64(len(configBlob)), nil
	}
	return nil, 0, errors.New("No blob with specified digest found in image")
}

func getV1Image(image types.Image) (imageV1 v1.Image, err error) {
	imageV1.OS = "linux"
	imageV1.Architecture = image.Arch
	imageV1.Config = image.ImageConfig
	imageV1.Created = image.Created

	for _, layer := range image.Layers {
		digest, err := godigest.Parse(layer.DiffIDs)
		if err != nil {
			return imageV1, err
		}
		imageV1.RootFS.DiffIDs = append(
			imageV1.RootFS.DiffIDs,
			digest)
		imageV1.RootFS.Type = "layers"
		// Even if optional in the spec, we
		// need to add an history otherwise
		// some toolings can complain:
		// https://github.com/nlewo/nix2container/issues/57
		imageV1.History = append(
			imageV1.History,
			layer.History,
		)
	}
	return
}

// NewImageFromFile creates an Image from a JSON file describing an
// image. This file has usually been created by Nix through the
// nix2container binary.
//
// The returned Image contains the layer metadata as-is from the JSON
// file.  Reproducible layers (those with Paths but no LayerPath)
// still reference nix store paths and will regenerate their tar
// on-the-fly when GetBlob is called.
//
// For push workflows where digest consistency is critical, callers
// should follow up with MaterializeReproducibleLayers to write each
// reproducible layer's tar to disk once and serve those exact bytes.
func NewImageFromFile(filename string) (image types.Image, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return image, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return image, err
	}
	err = json.Unmarshal(content, &image)
	if err != nil {
		return image, err
	}
	return image, nil
}

// MaterializeReproducibleLayers writes the tar for each reproducible
// layer to a temporary directory and updates the layer to reference
// that file. A layer is reproducible when it has Paths (tar is
// generated on-the-fly from nix store paths) and no LayerPath (no
// pre-built tar on disk).
//
// By materializing to disk we guarantee that GetBlob serves the
// exact same bytes that were hashed to produce the manifest digest.
// Previously, TarPaths was called twice (once here for the digest,
// once in GetBlob for the content) and any non-determinism between
// the two passes caused "Digest did not match" errors.
//
// Returns the path of the temporary directory (empty string if no
// reproducible layers exist). The caller must remove it when done.
func MaterializeReproducibleLayers(image *types.Image) (string, error) {
	// Quick scan: bail out early if there are no reproducible layers.
	hasReproducible := false
	for _, layer := range image.Layers {
		if layer.Paths != nil && layer.LayerPath == "" {
			hasReproducible = true
			break
		}
	}
	if !hasReproducible {
		return "", nil
	}

	tmpDir, err := os.MkdirTemp("", "n2c-layers-")
	if err != nil {
		return "", fmt.Errorf("creating temp dir for layer tars: %w", err)
	}

	for i := range image.Layers {
		layer := &image.Layers[i]
		if layer.Paths == nil || layer.LayerPath != "" {
			continue
		}
		lp, d, sz, err := TarPathsWrite(layer.Paths, tmpDir)
		if err != nil {
			os.RemoveAll(tmpDir)
			return "", fmt.Errorf("materializing layer %d: %w", i, err)
		}
		if layer.Digest != d.String() {
			logrus.Infof("Layer %d digest updated: %s -> %s (store paths differ from build time)", i, layer.Digest, d.String())
		}
		layer.Digest = d.String()
		layer.DiffIDs = d.String()
		layer.Size = sz
		layer.LayerPath = lp
	}
	return tmpDir, nil
}

// NewImageFromDir builds an Image based on an directory populated by
// the Skopeo dir transport. The directory needs to be a absolute
// path since tarball filepaths are referenced in the image Layers.
func NewImageFromDir(directory string) (image types.Image, err error) {
	image.Version = types.ImageVersion

	manifestFile, err := os.Open(directory + "/manifest.json")
	if err != nil {
		return image, err
	}
	defer manifestFile.Close()
	content, err := io.ReadAll(manifestFile)
	if err != nil {
		return image, err
	}
	var v1Manifest v1.Manifest
	err = json.Unmarshal(content, &v1Manifest)
	if err != nil {
		return image, err
	}

	content, err = os.ReadFile(directory + "/" + v1Manifest.Config.Digest.Encoded())
	if err != nil {
		return image, err
	}
	var v1ImageConfig manifest.Schema2Image
	err = json.Unmarshal(content, &v1ImageConfig)
	if err != nil {
		return image, err
	}

	// TODO: we should also load the configuration in order to
	// allow configuration merges

	for i, l := range v1Manifest.Layers {
		layerFilename := directory + "/" + l.Digest.Encoded()
		logrus.Infof("Adding tar file '%s' as image layer", layerFilename)
		layer := types.Layer{
			LayerPath: layerFilename,
			Digest:    l.Digest.String(),
			DiffIDs:   v1ImageConfig.RootFS.DiffIDs[i].String(),
		}
		switch l.MediaType {
		case "application/vnd.docker.image.rootfs.diff.tar":
			layer.MediaType = v1.MediaTypeImageLayer
		case "application/vnd.docker.image.rootfs.diff.tar.gzip":
			layer.MediaType = v1.MediaTypeImageLayerGzip
		case "application/vnd.oci.image.layer.v1.tar":
			layer.MediaType = l.MediaType
		case "application/vnd.oci.image.layer.v1.tar+gzip":
			layer.MediaType = l.MediaType
		case "application/vnd.oci.image.layer.v1.tar+zstd":
			layer.MediaType = l.MediaType
		default:
			return image, fmt.Errorf("Unsupported media type: %q", l.MediaType)
		}
		image.Layers = append(image.Layers, layer)
	}
	return image, nil
}

// NewImageFromManifest builds an Image based on a registry manifest
// and a separate JSON mapping pointing to the locations of the
// associated blobs (layer archives).
func NewImageFromManifest(manifestFilename string, blobMapFilename string) (image types.Image, err error) {
	image.Version = types.ImageVersion

	content, err := os.ReadFile(manifestFilename)
	if err != nil {
		return image, err
	}
	var v1Manifest v1.Manifest
	err = json.Unmarshal(content, &v1Manifest)
	if err != nil {
		return image, err
	}

	var blobMap map[string]string
	content, err = os.ReadFile(blobMapFilename)
	if err != nil {
		return image, err
	}
	err = json.Unmarshal(content, &blobMap)
	if err != nil {
		return image, err
	}

	var configFilename = blobMap[v1Manifest.Config.Digest.Encoded()]
	content, err = os.ReadFile(configFilename)
	if err != nil {
		return image, err
	}
	var v1ImageConfig manifest.Schema2Image
	err = json.Unmarshal(content, &v1ImageConfig)
	if err != nil {
		return image, err
	}

	for i, l := range v1Manifest.Layers {
		layerFilename := blobMap[l.Digest.Encoded()]
		logrus.Infof("Adding tar file '%s' as image layer", layerFilename)
		layer := types.Layer{
			LayerPath: layerFilename,
			Digest:    l.Digest.String(),
			DiffIDs:   v1ImageConfig.RootFS.DiffIDs[i].String(),
		}
		switch l.MediaType {
		case "application/vnd.docker.image.rootfs.diff.tar":
			layer.MediaType = v1.MediaTypeImageLayer
		case "application/vnd.docker.image.rootfs.diff.tar.gzip":
			layer.MediaType = v1.MediaTypeImageLayerGzip
		case "application/vnd.oci.image.layer.v1.tar":
			layer.MediaType = l.MediaType
		case "application/vnd.oci.image.layer.v1.tar+gzip":
			layer.MediaType = l.MediaType
		case "application/vnd.oci.image.layer.v1.tar+zstd":
			layer.MediaType = l.MediaType
		default:
			return image, fmt.Errorf("Unsupported media type: %q", l.MediaType)
		}
		image.Layers = append(image.Layers, layer)
	}
	return image, nil
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }
