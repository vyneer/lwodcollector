package main

import (
	"encoding/json"
	"os"
	"time"

	apex "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	_ "github.com/mattn/go-sqlite3"
	flag "github.com/spf13/pflag"
	"github.com/vyneer/lwodcollector/config"
	log "github.com/vyneer/lwodcollector/logger"
	"github.com/vyneer/lwodcollector/parser"
	"github.com/vyneer/lwodcollector/util"
)

var cfg config.Config

func init() {
	log.SetHandler(text.New((os.Stderr)))

	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatalf("%s", err)
	}
	time.Local = loc

	flag.BoolVarP(&cfg.Flags.Verbose, "verbose", "v", false, "Show debug messages")
	flag.BoolVarP(&cfg.Flags.AllSheets, "all", "a", false, "Process every single sheet")
	flag.IntVarP(&cfg.Flags.Continuous, "continuous", "c", 0, "Run the parser every set amount of minutes (can also be set with the REFRESH env var)")
	flag.Lookup("continuous").NoOptDefVal = "60"
}

func loop() {
	config.CreateGoogleClients(&cfg)
	config.LoadDatabase(&cfg)

	sheets, err := parser.CollectSheets(cfg)
	if err != nil {
		log.Fatalf("%v", err)
	}
	_, okToday := sheets["Today"]
	_, okOneMonthAgo := sheets["OneMonthAgo"]
	_, okPlusSixDays := sheets["PlusSixDays"]
	if okToday || okOneMonthAgo || okPlusSixDays {
		sheetsPretty, err := json.MarshalIndent(sheets, "", "	")
		if err != nil {
			log.Fatalf("%v", err)
		}
		log.Infof("Grabbed the sheets from the folder: %+v", string(sheetsPretty))
	} else {
		log.Infof("Grabbed the sheets from the folder: %+v", sheets)
	}
	parser.ParseSheets(sheets, cfg)
	if cfg.HealthCheck != "" && cfg.Flags.Continuous != 0 {
		util.HealthCheck(cfg.HealthCheck)
	}
}

func main() {
	cfg = config.LoadDotEnv()

	flag.Parse()
	if cfg.Flags.Verbose {
		log.SetLevel(apex.DebugLevel)
	}

	switch cfg.Flags.Continuous {
	case 0:
		if cfg.Refresh != 0 {
			cfg.Flags.Continuous = cfg.Refresh
		}
	case 60:
		if cfg.Refresh != 0 {
			cfg.Flags.Continuous = cfg.Refresh
		}
	}

	if cfg.Flags.Continuous != 0 && cfg.Flags.AllSheets {
		log.Fatalf("Can't continuously run the parser on every single sheet, please either remove the -a flag, or the -c flag (or the REFRESH env var)")
	}

	if cfg.Flags.Continuous != 0 {
		log.Infof("Running the application in continuous mode, refreshing every %d minute(s)", cfg.Flags.Continuous)
		for {
			loop()
			time.Sleep(time.Second * 60 * time.Duration(cfg.Flags.Continuous))
		}
	} else {
		loop()
	}
}
