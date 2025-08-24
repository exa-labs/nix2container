// The generated structure is a list of layers. Currently, the list
// always contains a single Layer, but in the future, we would like to
// generate several layers with some algorithms, such as
// https://grahamc.com/blog/nix-and-layered-docker-images

package cmd

import (
	_ "crypto/sha256"
	_ "crypto/sha512"
	"encoding/json"
	"fmt"
	"os"
    "time"

	"github.com/nlewo/nix2container/closure"
	"github.com/nlewo/nix2container/nix"
	"github.com/nlewo/nix2container/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

var ignore string
var tarDirectory string
var permsFilepath string
var rewritesFilepath string
var historyFilepath string
var maxLayers int
var sortBy string

// layerCmd represents the layer command
var layersReproducibleCmd = &cobra.Command{
	Use:   "layers-from-reproducible-storepaths OUTPUT-FILENAME.JSON CLOSURE-GRAPH.JSON",
	Short: "Generate a layers.json file from a list of reproducible paths",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
        start := time.Now()
        logrus.Infof("nix2container: reproducible layering start (sortBy=%s, maxLayers=%d)", sortBy, maxLayers)

        step := time.Now()
		closureGraph, err := closure.ReadClosureGraphFile(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("read closure graph: %v", time.Since(step))
		var storepaths []string
        step = time.Now()
		switch sortBy {
		case "nar-size":
			storepaths, err = closure.SortedPathsByNarSize(closureGraph)
		default:
			storepaths, err = closure.SortedPathsByPopularity(closureGraph)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("sorted storepaths (%s): %v (count=%d)", sortBy, time.Since(step), len(storepaths))

        step = time.Now()
		parents, err := getLayersFromFiles(args[2:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("loaded parents: %v (parents=%d)", time.Since(step), len(parents))
		var perms []types.PermPath
		if permsFilepath != "" {
            step = time.Now()
			perms, err = readPermsFile(permsFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read perms: %v (entries=%d)", time.Since(step), len(perms))
		}
		var rewrites []types.RewritePath
		if rewritesFilepath != "" {
            step = time.Now()
			rewrites, err = readRewritesFile(rewritesFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read rewrites: %v (entries=%d)", time.Since(step), len(rewrites))
		}
		var history v1.History
		if historyFilepath != "" {
            step = time.Now()
			history, err = readHistoryFile(historyFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read history: %v", time.Since(step))
		}
        step = time.Now()
		layers, err := nix.NewLayers(storepaths, maxLayers, parents, rewrites, ignore, perms, history)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("build layers: %v (layers=%d)", time.Since(step), len(layers))

        step = time.Now()
		err = layersToJson(args[0], layers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("write layers.json: %v", time.Since(step))
        logrus.Infof("total (reproducible): %v", time.Since(start))
	},
}

// layerCmd represents the layer command
var layersNonReproducibleCmd = &cobra.Command{
	Use:   "layers-from-non-reproducible-storepaths OUTPUT-FILENAME.JSON CLOSURE-GRAPH.JSON",
	Short: "Generate a layers.json file from a list of paths",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
        start := time.Now()
        logrus.Infof("nix2container: non-reproducible layering start (sortBy=%s, maxLayers=%d, tarDir=%s)", sortBy, maxLayers, tarDirectory)

        step := time.Now()
		closureGraph, err := closure.ReadClosureGraphFile(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("read closure graph: %v", time.Since(step))
		var storepaths []string
        step = time.Now()
		switch sortBy {
		case "nar-size":
			storepaths, err = closure.SortedPathsByNarSize(closureGraph)
		default:
			storepaths, err = closure.SortedPathsByPopularity(closureGraph)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("sorted storepaths (%s): %v (count=%d)", sortBy, time.Since(step), len(storepaths))

        step = time.Now()
		parents, err := getLayersFromFiles(args[2:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("loaded parents: %v (parents=%d)", time.Since(step), len(parents))
		var perms []types.PermPath
		if permsFilepath != "" {
            step = time.Now()
			perms, err = readPermsFile(permsFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read perms: %v (entries=%d)", time.Since(step), len(perms))
		}
		var rewrites []types.RewritePath
		if rewritesFilepath != "" {
            step = time.Now()
			rewrites, err = readRewritesFile(rewritesFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read rewrites: %v (entries=%d)", time.Since(step), len(rewrites))
		}
		var history v1.History
		if historyFilepath != "" {
            step = time.Now()
			history, err = readHistoryFile(historyFilepath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}
            logrus.Infof("read history: %v", time.Since(step))
		}
        step = time.Now()
		layers, err := nix.NewLayersNonReproducible(storepaths, maxLayers, tarDirectory, parents, rewrites, ignore, perms, history)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("build layers (non-reproducible): %v (layers=%d)", time.Since(step), len(layers))

        step = time.Now()
		err = layersToJson(args[0], layers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
        logrus.Infof("write layers.json: %v", time.Since(step))
        logrus.Infof("total (non-reproducible): %v", time.Since(start))
	},
}

func layersToJson(outputFilename string, layers []types.Layer) error {
	res, err := json.MarshalIndent(layers, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(outputFilename, []byte(res), 0666)
	if err != nil {
		return err
	}
	logrus.Infof("Layers have been written to %s", outputFilename)
	return nil
}

func getLayersFromFiles(layersPaths []string) (layers []types.Layer, err error) {
	for _, layersPath := range layersPaths {
		ls, err := types.NewLayersFromFile(layersPath)
		if err != nil {
			return layers, err
		}
		layers = append(layers, ls...)
	}
	return layers, nil
}

func init() {
	rootCmd.AddCommand(layersNonReproducibleCmd)
	layersNonReproducibleCmd.Flags().StringVarP(&ignore, "ignore", "", "", "Ignore the path from the list of storepaths")
	// TODO: make this flag required
	layersNonReproducibleCmd.Flags().StringVarP(&tarDirectory, "tar-directory", "", "", "The directory where tar of layers are created.")

	layersNonReproducibleCmd.Flags().StringVarP(&rewritesFilepath, "rewrites", "", "", "A JSON file containing a list of path rewrites. Each element of the list is a JSON object with the attributes path, regex and repl: for a given path, the regex is replaced by repl.")
	layersNonReproducibleCmd.Flags().StringVarP(&permsFilepath, "perms", "", "", "A JSON file containing file permissions")
	layersNonReproducibleCmd.Flags().StringVarP(&historyFilepath, "history", "", "", "A JSON file containing layer history")
	layersNonReproducibleCmd.Flags().IntVarP(&maxLayers, "max-layers", "", 1, "The maximum number of layers")
	layersNonReproducibleCmd.Flags().StringVarP(&sortBy, "sort-by", "", "popularity", "Sort store paths by: popularity|nar-size")

	rootCmd.AddCommand(layersReproducibleCmd)
	layersReproducibleCmd.Flags().StringVarP(&ignore, "ignore", "", "", "Ignore the path from the list of storepaths")
	layersReproducibleCmd.Flags().StringVarP(&rewritesFilepath, "rewrites", "", "", "A JSON file containing path rewrites")
	layersReproducibleCmd.Flags().StringVarP(&permsFilepath, "perms", "", "", "A JSON file containing file permissions")
	layersReproducibleCmd.Flags().StringVarP(&historyFilepath, "history", "", "", "A JSON file containing layer history")
	layersReproducibleCmd.Flags().IntVarP(&maxLayers, "max-layers", "", 1, "The maximum number of layers")
	layersReproducibleCmd.Flags().StringVarP(&sortBy, "sort-by", "", "popularity", "Sort store paths by: popularity|nar-size")

}
