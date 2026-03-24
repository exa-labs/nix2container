package nix

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/image"
	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/transports"
	"github.com/containers/image/v5/types"
	n2c "github.com/nlewo/nix2container/nix"
	n2ctypes "github.com/nlewo/nix2container/types"
	digest "github.com/opencontainers/go-digest"
	imgspecv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

func init() {
	transports.Register(Transport)
}

// Transport is an ImageTransport for nix2container JSON image specification.
var Transport = nixTransport{}

type nixTransport struct{}

func (t nixTransport) Name() string {
	return "nix"
}

type nixReference struct {
	path     string
	nixImage n2ctypes.Image
}

// ParseReference converts a string, which should not start with the ImageTransport.Name prefix, into an ImageReference.
func (t nixTransport) ParseReference(reference string) (types.ImageReference, error) {
	nixImage, err := n2c.NewImageFromFile(reference)
	if err != nil {
		return nil, err
	}
	imageReference := nixReference{
		path:     reference,
		nixImage: nixImage,
	}
	return imageReference, nil
}

// ValidatePolicyConfigurationScope checks that scope is a valid name for a signature.PolicyTransportScopes keys
func (t nixTransport) ValidatePolicyConfigurationScope(scope string) error {
	return errors.New(`nix: does not support any scopes except the default "" one`)
}

// StringWithinTransport returns a string representation of the reference.
func (ref nixReference) StringWithinTransport() string {
	return fmt.Sprintf("%s", ref.path)
}

func (ref nixReference) Transport() types.ImageTransport {
	return Transport
}

// DeleteImage deletes the named image from the registry, if supported.
func (ref nixReference) DeleteImage(ctx context.Context, sys *types.SystemContext) error {
	return errors.New("Deleting images not implemented for nix: images")
}

// DockerReference returns a Docker reference associated with this reference.
func (ref nixReference) DockerReference() reference.Named {
	return nil
}

// NewImage returns a types.ImageCloser for this reference.
func (ref nixReference) NewImage(ctx context.Context, sys *types.SystemContext) (types.ImageCloser, error) {
	src, err := newImageSource(ctx, sys, ref)
	if err != nil {
		return nil, err
	}
	return image.FromSource(ctx, sys, src)
}

// NewImageSource returns a types.ImageSource for this reference.
func (ref nixReference) NewImageSource(ctx context.Context, sys *types.SystemContext) (types.ImageSource, error) {
	return newImageSource(ctx, sys, ref)
}

// PolicyConfigurationIdentity returns a string representation of the reference, suitable for policy lookup.
func (ref nixReference) PolicyConfigurationIdentity() string {
	return ""
}

// PolicyConfigurationNamespaces returns a list of other policy configuration namespaces to search.
func (ref nixReference) PolicyConfigurationNamespaces() []string {
	return []string{}
}

// NewImageDestination returns a types.ImageDestination for this reference.
func (ref nixReference) NewImageDestination(ctx context.Context, sys *types.SystemContext) (types.ImageDestination, error) {
	return nil, errors.New("It is not possible to copy to nix: images")
}

func newImageSource(ctx context.Context, sys *types.SystemContext, ref nixReference) (types.ImageSource, error) {
	// Recompute layer digests from actual blob content so the manifest
	// always matches the bytes that GetBlob will serve. This prevents
	// "Digest did not match" errors when the pre-computed digest in the
	// image JSON diverges from the tar bytes produced at push time.
	if err := n2c.RecomputeLayerDigests(&ref.nixImage); err != nil {
		return nil, fmt.Errorf("recomputing layer digests: %w", err)
	}
	return &nixImageSource{
		ref: ref,
	}, nil
}

type nixImageSource struct {
	ref nixReference
}

// Close removes resources associated with an initialized ImageSource, if any.
func (s *nixImageSource) Close() error {
	return nil
}

func (s *nixImageSource) GetBlob(ctx context.Context, info types.BlobInfo, cache types.BlobInfoCache) (io.ReadCloser, int64, error) {
	rc, _, err := n2c.GetBlob(s.ref.nixImage, info.Digest)
	return rc, -1, err
}

// GetManifest returns the image's manifest along with its MIME type.
func (s *nixImageSource) GetManifest(ctx context.Context, instanceDigest *digest.Digest) ([]byte, string, error) {
	configDigest, size, err := n2c.GetConfigDigest(s.ref.nixImage)
	if err != nil {
		return nil, "", err
	}
	config := imgspecv1.Descriptor{
		MediaType: imgspecv1.MediaTypeImageConfig,
		Size:      size,
		Digest:    configDigest,
	}

	var layers []imgspecv1.Descriptor
	for _, layer := range s.ref.nixImage.Layers {
		digest, err := digest.Parse(layer.Digest)
		if err != nil {
			return nil, "", err
		}
		layers = append(layers, imgspecv1.Descriptor{
			MediaType: layer.MediaType,
			Size:      layer.Size,
			Digest:    digest,
		})
	}

	m := manifest.OCI1FromComponents(
		config,
		layers,
	)
	manifestBytes, err := json.Marshal(&m)
	if err != nil {
		return nil, "", err
	}
	return manifestBytes, imgspecv1.MediaTypeImageManifest, nil
}

func (s *nixImageSource) GetSignatures(ctx context.Context, instanceDigest *digest.Digest) ([][]byte, error) {
	return [][]byte{}, nil
}

// HasThreadSafeGetBlob indicates whether GetBlob can be executed concurrently.
func (s *nixImageSource) HasThreadSafeGetBlob() bool {
	return true
}

// LayerInfosForCopy returns either nil (meaning the values in the manifest are fine), or updated values for the layer
// blobsums that are listed in the image's manifest.
func (s *nixImageSource) LayerInfosForCopy(ctx context.Context, instanceDigest *digest.Digest) ([]types.BlobInfo, error) {
	return nil, nil
}

// Reference returns the reference used to set up this source.
func (s *nixImageSource) Reference() types.ImageReference {
	return s.ref
}
