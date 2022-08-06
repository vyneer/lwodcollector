package main

import (
	"os"
	"sync"
	"time"

	apex "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	_ "github.com/mattn/go-sqlite3"
	flag "github.com/spf13/pflag"
	"github.com/vyneer/lwodcollector/config"
	"github.com/vyneer/lwodcollector/gsheets"
	log "github.com/vyneer/lwodcollector/logger"
	"github.com/vyneer/lwodcollector/util"
	"github.com/vyneer/lwodcollector/yt"
	"google.golang.org/api/youtube/v3"
)

var cfg config.Config
var defFlags *flag.FlagSet
var sheetsFlags *flag.FlagSet
var ytFlags *flag.FlagSet

func init() {
	log.SetHandler(text.New((os.Stderr)))

	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatalf("%s", err)
	}
	time.Local = loc

	defFlags = flag.NewFlagSet("for all subcommands", flag.ExitOnError)
	defFlags.BoolVarP(&cfg.Flags.Verbose, "verbose", "v", false, "Show debug messages")

	sheetsFlags = flag.NewFlagSet("LWOD", flag.ExitOnError)
	sheetsFlags.BoolVarP(&cfg.Flags.AllSheets, "all", "a", false, "Process every single sheet")
	sheetsFlags.AddFlagSet(defFlags)

	ytFlags = flag.NewFlagSet("YT", flag.ExitOnError)
	ytFlags.BoolVarP(&cfg.Flags.AllVideos, "all", "a", false, "Process every single video")
	ytFlags.AddFlagSet(defFlags)
}

func main() {
	cfg = config.Initialize()
	cfg.Continuous = false

	if len(os.Args) == 1 {
		log.Fatalf("No subcommand given")
	}

	switch os.Args[1] {
	case "continuous":
		defFlags.Parse(os.Args[2:])
		if cfg.Flags.Verbose {
			log.SetLevel(apex.DebugLevel)
		}

		cfg.Continuous = true
		cfg.Flags.AllSheets = false
		cfg.Flags.AllVideos = false

		var wg sync.WaitGroup
		sheetsSleepTime := time.Second * 60 * time.Duration(cfg.LWODRefresh)
		ytApiSleepTime := time.Second * 60 * time.Duration(cfg.YTAPIRefresh)
		ytSleepTime := time.Second * 60 * time.Duration(cfg.YTRefresh)
		log.Infof("Running the application in continuous mode, refreshing LWOD every %d minute(s), YT every %d minute(s)", cfg.LWODRefresh, cfg.YTRefresh)

		api := make(chan []*youtube.Video)
		scraped := make(chan []*youtube.Video)

		if cfg.YTAPIRefresh != 0 {
			wg.Add(1)
			util.StartYTThread("[YT] [API]", yt.LoopApiLivestream, &cfg, api, ytApiSleepTime)
		}

		if cfg.YTRefresh != 0 {
			wg.Add(1)
			util.StartYTThread("[YT] [SCRAPER]", yt.LoopScrapedLivestream, &cfg, scraped, ytSleepTime)
		}

		if cfg.YTRefresh != 0 {
			wg.Add(1)
			util.StartYTMainThread("[YT] [SCRAPER]", yt.LoopPlaylist, &cfg, api, scraped, ytSleepTime)
		}

		if cfg.LWODRefresh != 0 {
			wg.Add(1)
			util.StartSheetsThread("[LWOD]", gsheets.SheetsLoop, &cfg, sheetsSleepTime)
		}

		wg.Wait()
	case "youtube":
		ytFlags.Parse(os.Args[2:])
		if cfg.Flags.Verbose {
			log.SetLevel(apex.DebugLevel)
		}

		api := make(chan []*youtube.Video)
		scraped := make(chan []*youtube.Video)

		err := yt.LoopApiLivestream(&cfg, api)
		if err != nil {
			log.Errorf("[YT] [API] Got an error, shutting down: %v", err)
			os.Exit(2)
		}

		err = yt.LoopScrapedLivestream(&cfg, scraped)
		if err != nil {
			log.Errorf("[YT] [SCRAPER] Got an error, shutting down: %v", err)
			os.Exit(2)
		}

		err = yt.LoopPlaylist(&cfg, api, scraped)
		if err != nil {
			log.Errorf("[YT] Got an error, shutting down: %v", err)
			os.Exit(2)
		}
	case "lwod":
		sheetsFlags.Parse(os.Args[2:])
		if cfg.Flags.Verbose {
			log.SetLevel(apex.DebugLevel)
		}

		err := gsheets.SheetsLoop(&cfg)
		if err != nil {
			log.Errorf("[LWOD] Got an error, shutting down: %v", err)
			os.Exit(2)
		}
	default:
		log.Errorf("%q is not a valid subcommand, valid:\n- lwod\n- youtube\n- continuous", os.Args[1])
		os.Exit(2)
	}
}
