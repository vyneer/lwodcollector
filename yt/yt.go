package yt

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gocolly/colly/v2"
	"github.com/vyneer/lwodcollector/config"
	log "github.com/vyneer/lwodcollector/logger"
	"github.com/vyneer/lwodcollector/util"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/youtube/v3"
)

type initialData struct {
	CurrentVideoEndpoint struct {
		WatchEndpoint struct {
			VideoID string `json:"videoId"`
		} `json:"watchEndpoint"`
	} `json:"currentVideoEndpoint"`
}

type YTVod struct {
	ID             string
	PubTime        string
	Title          string
	StartTime      string
	EndTime        string
	Thumbnail      string
	LivestreamEtag string
	Hash           string
}

var ErrIsNotModified = errors.New("not modified")

func ScrapeLivestreamID(config *config.Config) string {
	var index int
	var id string
	c := colly.NewCollector()
	// disable cookie handling to bypass youtube consent screen
	c.DisableCookies()

	c.OnResponse(func(r *colly.Response) {
		index = strings.Index(string(r.Body), "Started streaming ")
	})

	c.OnHTML("script", func(h *colly.HTMLElement) {
		if !strings.Contains(h.Text, "var ytInitialData") {
			return
		}

		if index == -1 {
			return
		}

		initialDataString := strings.TrimPrefix(h.Text, "var ytInitialData = ")
		initialDataString = strings.TrimSuffix(initialDataString, ";")

		var initialData initialData
		if err := json.Unmarshal([]byte(initialDataString), &initialData); err != nil {
			return
		}

		id = initialData.CurrentVideoEndpoint.WatchEndpoint.VideoID
	})

	c.Visit(fmt.Sprintf("https://youtube.com/channel/%s/live?hl=en", config.YTChannel))

	return id
}

func GetLivestreamID(config *config.Config, etag string) ([]*youtube.Video, string, error) {
	resp, err := config.GoogleConfig.YouTube.Search.List([]string{"snippet"}).IfNoneMatch(etag).EventType("live").ChannelId(config.YTChannel).Type("video").Do()
	if err != nil {
		if !googleapi.IsNotModified(err) {
			return nil, etag, WrapWithYTError(err, "API", "Youtube API error")
		} else {
			return nil, etag, WrapWithYTError(ErrIsNotModified, "API", "Got a 304 Not Modified for livestream ID, returning an empty slice")
		}
	}

	if len(resp.Items) > 0 {
		id, _, err := GetVideoInfo(config, resp.Items[0].Id.VideoId, "")
		if err != nil && !errors.Is(err, ErrIsNotModified) {
			return id, resp.Etag, nil
		}
		return id, resp.Etag, nil
	} else {
		return nil, resp.Etag, nil
	}
}

func GetVideoInfo(config *config.Config, id string, etag string) ([]*youtube.Video, string, error) {
	resp, err := config.GoogleConfig.YouTube.Videos.List([]string{"snippet", "liveStreamingDetails"}).IfNoneMatch(etag).Id(id).Do()
	if err != nil {
		if !googleapi.IsNotModified(err) {
			return nil, etag, WrapWithYTError(err, "", "Youtube API error")
		} else {
			return nil, etag, WrapWithYTError(ErrIsNotModified, "", "Got a 304 Not Modified for full video info, returning an empty slice")
		}
	}

	return resp.Items, resp.Etag, nil
}

func GetLivestreamInfo(config *config.Config, id string, etag string) ([]*youtube.Video, string, error) {
	resp, err := config.GoogleConfig.YouTube.Videos.List([]string{"liveStreamingDetails"}).IfNoneMatch(etag).Id(id).Do()
	if err != nil {
		if !googleapi.IsNotModified(err) {
			return nil, etag, WrapWithYTError(err, "", "Youtube API error")
		} else {
			return nil, etag, WrapWithYTError(ErrIsNotModified, "", "Got a 304 Not Modified for livestream info, returning an empty slice")
		}
	}

	return resp.Items, resp.Etag, nil
}

func GetPlaylistVideos(config *config.Config, etag string) ([]*youtube.PlaylistItem, string, error) {
	if !config.Flags.AllVideos {
		resp, err := config.GoogleConfig.YouTube.PlaylistItems.List([]string{"snippet", "contentDetails"}).IfNoneMatch(etag).MaxResults(45).PlaylistId(config.YTPlaylist).Do()
		if err != nil {
			if !googleapi.IsNotModified(err) {
				return nil, etag, WrapWithYTError(err, "", "Youtube API error")
			} else {
				return nil, etag, WrapWithYTError(ErrIsNotModified, "", "Got a 304 Not Modified for the playlist, returning an empty slice")
			}
		}

		return resp.Items, resp.Etag, nil
	} else {
		var items []*youtube.PlaylistItem
		var err error
		resp, err := config.GoogleConfig.YouTube.PlaylistItems.List([]string{"snippet", "contentDetails"}).MaxResults(50).PlaylistId(config.YTPlaylist).Do()
		if err != nil {
			if !googleapi.IsNotModified(err) {
				return nil, etag, WrapWithYTError(err, "", "Youtube API error")
			}
		}
		items = append(items, resp.Items...)
		for resp.NextPageToken != "" {
			resp, err = config.GoogleConfig.YouTube.PlaylistItems.List([]string{"snippet", "contentDetails"}).PageToken(resp.NextPageToken).MaxResults(50).PlaylistId(config.YTPlaylist).Do()
			if err != nil {
				if !googleapi.IsNotModified(err) {
					return nil, etag, WrapWithYTError(err, "", "Youtube API error")
				}
			}
			items = append(items, resp.Items...)
		}

		return items, "", nil
	}

}

func GetVideosInDB(config *config.Config) ([]YTVod, error) {
	var vods []YTVod

	rows, err := config.YTDBConfig.Statements.SelectVods.Query()
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			log.Debugf("[YT] Couldn't find any rows in the ytvod DB.")
			return nil, nil
		default:
			return nil, WrapWithYTError(err, "", "Sqlite error")
		}
	}
	defer rows.Close()

	for rows.Next() {
		var vod YTVod
		err := rows.Scan(&vod.ID, &vod.PubTime, &vod.Title, &vod.StartTime, &vod.EndTime, &vod.Thumbnail, &vod.LivestreamEtag, &vod.Hash)
		if err != nil {
			return nil, WrapWithYTError(err, "", "Sqlite error")
		}
		vods = append(vods, vod)
	}

	return vods, nil
}

func UpdateEverythingVideo(config *config.Config, video *youtube.Video, vod YTVod) error {
	if video.Snippet.ChannelId == config.YTChannel {
		vid := video.Id
		pubtime := video.Snippet.PublishedAt
		title := video.Snippet.Title
		thumbnail := video.Snippet.Thumbnails.Medium.Url
		if video.LiveStreamingDetails.ActualStartTime == "" {
			info, livestreamEtag, err := GetLivestreamInfo(config, vid, vod.LivestreamEtag)
			if err != nil {
				switch {
				case errors.Is(err, ErrIsNotModified):
					log.Debugf("[YT] Got a 304 Not Modified for livestream info for ID %s, skipping", vid)
				default:
					return WrapWithYTError(err, "", "Couldn't get livestream info")
				}
			}
			for _, v := range info {
				starttime := v.LiveStreamingDetails.ActualStartTime
				if starttime != "" {
					endtime, err := CalculateEndTime(v.LiveStreamingDetails.ActualEndTime, starttime)
					if err != nil {
						log.Errorf("[YT] Couldn't parse the starttime for VOD with ID %s: %v", vod.ID, err)
					}

					hashString := vid + pubtime + title + starttime + endtime + thumbnail + livestreamEtag
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					if hashNew != vod.Hash {
						_, err := config.YTDBConfig.Statements.ReplaceVod.Exec(vid, pubtime, title, starttime, endtime, thumbnail, livestreamEtag, hashNew)
						if err != nil {
							return WrapWithYTError(err, "", fmt.Sprintf("Couldn't replace VOD with Youtube ID %s", vid))
						}
						log.Debugf("[YT] Added/updated the VOD with ID %s", vid)
					} else {
						log.Debugf("[YT] VOD with ID %s not changed, skipping", vid)
					}
				} else {
					log.Debugf("[YT] Video with Youtube ID %s doesn't have livestream info, skipping", vid)
				}
			}
		} else {
			starttime := video.LiveStreamingDetails.ActualStartTime
			if starttime != "" {
				endtime, err := CalculateEndTime(video.LiveStreamingDetails.ActualEndTime, starttime)
				if err != nil {
					log.Errorf("[YT] Couldn't parse the starttime for VOD with ID %s: %v", vod.ID, err)
				}

				hashString := vid + pubtime + title + starttime + endtime + thumbnail + video.Etag
				hashNewUint64 := xxhash.Sum64String(hashString)
				hashNew := strconv.FormatUint(hashNewUint64, 10)
				if hashNew != vod.Hash {
					_, err := config.YTDBConfig.Statements.ReplaceVod.Exec(vid, pubtime, title, starttime, endtime, thumbnail, video.Etag, hashNew)
					if err != nil {
						return WrapWithYTError(err, "", fmt.Sprintf("Couldn't replace VOD with Youtube ID %s", vid))
					}
					log.Debugf("[YT] Added/updated the VOD with ID %s", vid)
				} else {
					log.Debugf("[YT] VOD with ID %s not changed, skipping", vid)
				}
			} else {
				log.Debugf("[YT] Video with Youtube ID %s doesn't have livestream info, skipping", vid)
			}
		}
	} else {
		log.Debugf("[YT] Video with Youtube ID %s is private, skipping", video.Id)
	}
	return nil
}

func UpdateEverythingPlaylist(config *config.Config, playlistElement *youtube.PlaylistItem, vod YTVod) error {
	if playlistElement.Snippet.VideoOwnerChannelId == config.YTChannel {
		vid := playlistElement.Snippet.ResourceId.VideoId
		pubtime := playlistElement.ContentDetails.VideoPublishedAt
		title := playlistElement.Snippet.Title
		thumbnail := playlistElement.Snippet.Thumbnails.Medium.Url
		info, livestreamEtag, err := GetLivestreamInfo(config, vid, vod.LivestreamEtag)
		if err != nil {
			switch {
			case errors.Is(err, ErrIsNotModified):
				log.Debugf("[YT] Got a 304 Not Modified for livestream info for ID %s, skipping", vid)
			default:
				return WrapWithYTError(err, "", "Couldn't get livestream info")
			}
		}
		for _, v := range info {
			starttime := v.LiveStreamingDetails.ActualStartTime
			if starttime != "" {
				endtime, err := CalculateEndTime(v.LiveStreamingDetails.ActualEndTime, starttime)
				if err != nil {
					log.Errorf("[YT] Couldn't parse the starttime for VOD with ID %s: %v", vod.ID, err)
				}

				hashString := vid + pubtime + title + starttime + endtime + thumbnail + livestreamEtag
				hashNewUint64 := xxhash.Sum64String(hashString)
				hashNew := strconv.FormatUint(hashNewUint64, 10)
				if hashNew != vod.Hash {
					_, err := config.YTDBConfig.Statements.ReplaceVod.Exec(vid, pubtime, title, starttime, endtime, thumbnail, livestreamEtag, hashNew)
					if err != nil {
						return WrapWithYTError(err, "", fmt.Sprintf("Couldn't replace VOD with Youtube ID %s", vid))
					}
					log.Debugf("[YT] Added/updated the VOD with ID %s", vid)
				} else {
					log.Debugf("[YT] VOD with ID %s not changed, skipping", vid)
				}
			} else {
				log.Debugf("[YT] Video with Youtube ID %s doesn't have livestream info, skipping", vid)
			}
		}
	} else {
		log.Debugf("[YT] Video with Youtube ID %s is private, skipping", playlistElement.Snippet.ResourceId.VideoId)
	}
	return nil
}

func GetLivestreamSearchEtag(config *config.Config) (string, error) {
	var etag string
	err := config.YTDBConfig.Statements.GetLivestreamSearchEtag.QueryRow().Scan(&etag)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			log.Debugf("[YT] Couldn't find any Etags in the livestreamSearchEtag DB")
			return "", nil
		default:
			return "", WrapWithYTError(err, "", "Sqlite error")
		}
	}
	return etag, nil
}

func AddLivestreamSearchEtag(config *config.Config, etag string) error {
	_, err := config.YTDBConfig.Statements.AddLivestreamSearchEtag.Exec(time.Now(), etag)
	if err != nil {
		return WrapWithYTError(err, "", fmt.Sprintf("Couldn't add the livestream search Etag (%s)", etag))
	}
	return err
}

func GetPlaylistEtag(config *config.Config) (string, error) {
	var etag string
	err := config.YTDBConfig.Statements.GetPlaylistEtag.QueryRow().Scan(&etag)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			log.Debugf("[YT] Couldn't find any Etags in the playlistEtag DB")
			return "", nil
		default:
			return "", WrapWithYTError(err, "", "Sqlite error")
		}
	}
	return etag, nil
}

func AddPlaylistEtag(config *config.Config, etag string) error {
	_, err := config.YTDBConfig.Statements.AddPlaylistEtag.Exec(time.Now(), etag)
	if err != nil {
		return WrapWithYTError(err, "", fmt.Sprintf("Couldn't add the playlist Etag (%s)", etag))
	}
	return nil
}

func UpdatePlaylistInfo(config *config.Config, playlist []*youtube.PlaylistItem, vods []YTVod) error {
	for _, playlistElement := range playlist {
		time.Sleep(time.Second * time.Duration(config.YTDelay))
		index := VODIndex(vods, playlistElement.Snippet.ResourceId.VideoId)
		if index != -1 {
			err := UpdateEverythingPlaylist(config, playlistElement, vods[index])
			if err != nil {
				return err
			}
		} else {
			err := UpdateEverythingPlaylist(config, playlistElement, YTVod{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func LoopApiLivestream(config *config.Config, api chan []*youtube.Video) error {
	etagInit, err := GetLivestreamSearchEtag(config)
	if err != nil {
		return err
	}
	vid, etagEnd, err := GetLivestreamID(config, etagInit)
	if err != nil && !errors.Is(err, ErrIsNotModified) {
		return err
	}
	err = AddLivestreamSearchEtag(config, etagEnd)
	if err != nil {
		return err
	}
	if len(vid) > 0 {
		log.Debugf("[YT] [API] Found a currently running stream with ID %s", vid[0].Id)
		api <- vid
	} else {
		log.Debugf("[YT] [API] No stream found")
	}
	return nil
}

func LoopScrapedLivestream(config *config.Config, scraped chan []*youtube.Video) error {
	id := ScrapeLivestreamID(config)
	if id != "" {
		log.Debugf("[YT] [SCRAPER] Found a currently running stream with ID %s", id)
		vid, _, err := GetVideoInfo(config, id, "")
		if err != nil && !errors.Is(err, ErrIsNotModified) {
			return err
		}
		scraped <- vid
	} else {
		log.Debugf("[YT] [SCRAPER] No stream found")
	}
	return nil
}

func LoopPlaylist(config *config.Config, api chan []*youtube.Video, scraped chan []*youtube.Video) error {
	var playlistVideos []*youtube.PlaylistItem
	var playlistEtag string

	dbVideos, err := GetVideosInDB(config)
	if err != nil {
		return err
	}

	if !config.Flags.AllVideos {
		playlistEtag, err = GetPlaylistEtag(config)
		if err != nil {
			return err
		}
	}
	playlistVideos, playlistEtag, err = GetPlaylistVideos(config, playlistEtag)
	if err != nil {
		switch {
		case errors.Is(err, ErrIsNotModified):
			log.Debugf("[YT] Got a 304 Not Modified for the playlist, skipping all the processing")
		default:
			return err
		}
	}
	if !config.Flags.AllVideos {
		err = AddPlaylistEtag(config, playlistEtag)
		if err != nil {
			return err
		}
	}

	UpdatePlaylistInfo(config, playlistVideos, dbVideos)

outer:
	for {
		select {
		case apiVid := <-api:
			for _, v := range apiVid {
				log.Debugf("[YT] [API] Processing previously found current stream with ID %s", v.Id)
				index := VODIndex(dbVideos, v.Id)
				if index != -1 {
					UpdateEverythingVideo(config, v, dbVideos[index])
				} else {
					UpdateEverythingVideo(config, v, YTVod{})
				}
			}
		case scrapedVid := <-scraped:
			for _, v := range scrapedVid {
				log.Debugf("[YT] [SCRAPER] Processing previously found stream with ID %s", v.Id)
				index := VODIndex(dbVideos, v.Id)
				if index != -1 {
					UpdateEverythingVideo(config, v, dbVideos[index])
				} else {
					UpdateEverythingVideo(config, v, YTVod{})
				}
			}
		default:
			log.Debugf("[YT] No current stream to process")
			break outer
		}
	}

	endTimeLess := []YTVod{}
	for _, vod := range dbVideos {
		if vod.EndTime == "" {
			endTimeLess = append(endTimeLess, vod)
		}
	}
	if len(endTimeLess) != 0 {
		log.Debugf("[YT] Checking videos that don't have an 'endtime'...")
		for _, vod := range endTimeLess {
			vids, _, err := GetVideoInfo(config, vod.ID, vod.LivestreamEtag)
			if err != nil {
				log.Errorf("[YT] Got an error while checking endtime-less VOD with ID %s: %v", vod.ID, err)
				continue
			}
			if len(vids) != 0 {
				for _, vid := range vids {
					err = UpdateEverythingVideo(config, vid, vod)
					if err != nil {
						log.Errorf("[YT] Got an error while getting info for endtime-less VOD with ID %s: %v", vod.ID, err)
						continue
					}
				}
			} else {
				starttimeGo, err := time.Parse("2006-01-02T15:04:05Z", vod.StartTime)
				if err != nil {
					log.Errorf("[YT] Couldn't parse the time for endtime-less VOD with ID %s: %v", vod.ID, err)
					continue
				}
				endtime := starttimeGo.Add(time.Hour * 24).Format("2006-01-02T15:04:05Z")

				hashString := vod.ID + vod.PubTime + vod.Title + vod.StartTime + endtime + vod.Thumbnail + vod.LivestreamEtag
				hashNewUint64 := xxhash.Sum64String(hashString)
				hashNew := strconv.FormatUint(hashNewUint64, 10)
				if hashNew != vod.Hash {
					_, err := config.YTDBConfig.Statements.ReplaceVod.Exec(vod.ID, vod.PubTime, vod.Title, vod.StartTime, endtime, vod.Thumbnail, vod.LivestreamEtag, hashNew)
					if err != nil {
						log.Debugf("[YT] Couldn't replace VOD with Youtube ID %s: %v", vod.ID, err)
					}
					log.Debugf("[YT] Manually added the endtime (starttime+24h) to VOD with ID %s", vod.ID)
				} else {
					log.Debugf("[YT] VOD with ID %s not changed, skipping", vod.ID)
				}
			}
		}
	}

	if config.YTHealthCheck != "" && config.Continuous {
		util.HealthCheck(&config.YTHealthCheck)
	}
	return nil
}
