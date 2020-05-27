package cli

import (
	"log"

	"github.com/simplifi/anemometer/pkg/anemometer/config"
	"github.com/simplifi/anemometer/pkg/anemometer/monitor"
	"github.com/spf13/cobra"
)

var (
	configPath string
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Anemometer agent",
	Run: func(cmd *cobra.Command, args []string) {
		start()
	},
}

func init() {
	startCmd.Flags().StringVarP(
		&configPath,
		"config",
		"c",
		"/etc/anemometer.yml",
		"the full path to the yaml config file, default: /etc/anemometer.yml")
	rootCmd.AddCommand(startCmd)
}

// Starts up the agent
func start() {
	var exit = make(chan bool)

	log.Printf("INFO: Starting Anemometer")

	cfg, err := config.Read(configPath)
	if err != nil {
		log.Panicf("ERROR: Failed to load config: %v", err)
	}

	for _, mtConfig := range cfg.Monitors {
		mt, err := monitor.New(cfg.StatsdConfig, mtConfig)
		log.Printf("INFO: Launching monitor '%v'", mtConfig.Name)
		if err != nil {
			log.Panicf("ERROR: Failed to start monitor '%v': %v", mtConfig.Name, err)
		}
		go mt.Start()
	}
	// Block until something kills the process
	<-exit
}
