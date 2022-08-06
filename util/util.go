package util

import (
	"net/http"
	"time"

	"github.com/vyneer/lwodcollector/config"
	log "github.com/vyneer/lwodcollector/logger"
	"google.golang.org/api/youtube/v3"
)

type loopYT func(*config.Config, chan []*youtube.Video) error
type loopYTMain func(*config.Config, chan []*youtube.Video, chan []*youtube.Video) error
type loopSheets func(*config.Config) error

func StartSheetsThread(prefix string, f loopSheets, cfg *config.Config, sleeptime time.Duration) {
	go func() {
		timeout := 0

		for {
			if timeout > 0 {
				log.Infof("%s Sleeping for %d seconds before starting...", prefix, timeout)
				time.Sleep(time.Second * time.Duration(timeout))
			}
			err := f(cfg)
			if err != nil {
				log.Errorf("%s Got an error, will restart the loop: %v", prefix, err)
				switch {
				case timeout == 0:
					timeout = 1
				case (timeout >= 1 && timeout <= 32):
					timeout *= 2
				}
				continue
			}
			timeout = 0
			log.Infof("%s Sleeping for %.f minutes...", prefix, sleeptime.Minutes())
			time.Sleep(sleeptime)
		}
	}()
}

func StartYTThread(prefix string, f loopYT, cfg *config.Config, c chan []*youtube.Video, sleeptime time.Duration) {
	go func() {
		timeout := 0

		for {
			if timeout > 0 {
				log.Infof("%s Sleeping for %d seconds before starting...", prefix, timeout)
				time.Sleep(time.Second * time.Duration(timeout))
			}
			err := f(cfg, c)
			if err != nil {
				log.Errorf("%s Got an error, will restart the loop: %v", prefix, err)
				switch {
				case timeout == 0:
					timeout = 1
				case (timeout >= 1 && timeout <= 32):
					timeout *= 2
				}
				continue
			}
			timeout = 0
			log.Infof("%s Sleeping for %.f minutes...", prefix, sleeptime.Minutes())
			time.Sleep(sleeptime)
		}
	}()
}

func StartYTMainThread(prefix string, f loopYTMain, cfg *config.Config, c1 chan []*youtube.Video, c2 chan []*youtube.Video, sleeptime time.Duration) {
	go func() {
		timeout := 0

		for {
			if timeout > 0 {
				log.Infof("%s Sleeping for %d seconds before starting...", prefix, timeout)
				time.Sleep(time.Second * time.Duration(timeout))
			}
			err := f(cfg, c1, c2)
			if err != nil {
				log.Errorf("%s Got an error, will restart the loop: %v", prefix, err)
				switch {
				case timeout == 0:
					timeout = 1
				case (timeout >= 1 && timeout <= 32):
					timeout *= 2
				}
				continue
			}
			timeout = 0
			log.Infof("%s Sleeping for %.f minutes...", prefix, sleeptime.Minutes())
			time.Sleep(sleeptime)
		}
	}()
}

func HealthCheck(url *string) {
	var client = &http.Client{
		Timeout: 10 * time.Second,
	}

	_, err := client.Head(*url)
	if err != nil {
		log.Errorf("HealthCheck error: %s", err)
	}
}
