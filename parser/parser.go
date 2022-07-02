package parser

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/vyneer/lwodcollector/config"
	log "github.com/vyneer/lwodcollector/logger"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type LWODSheet struct {
	ID   string
	Name string
}

type LWODTemplate struct {
	Date, Start, End, Game, Subject, Topic, YouTube, Twitch int
}

type LWODEntry struct {
	DateAdded, DateStreamed                           time.Time
	YouTube, Twitch, Start, End, Game, Subject, Topic string
	YouTubeStamp                                      int
}

type LWODSheetData struct {
	YouTubeLinks, TwitchLinks map[string][]LWODEntry
}

func createTemplate(row []string) LWODTemplate {
	var template LWODTemplate
	tempReflectType := reflect.TypeOf(LWODTemplate{})

	for i, v := range row {
		lc := strings.ToLower(v)
		for k := 0; k < tempReflectType.NumField(); k++ {
			name := strings.ToLower(tempReflectType.Field(k).Name)
			switch name {
			case "twitch":
				if strings.Contains(lc, name) || strings.Contains(lc, "link") {
					reflect.ValueOf(&template).Elem().FieldByName(tempReflectType.Field(k).Name).SetInt(int64(i))
				}
			default:
				if strings.Contains(lc, name) {
					reflect.ValueOf(&template).Elem().FieldByName(tempReflectType.Field(k).Name).SetInt(int64(i))
				}
			}

		}
	}

	return template
}

func CollectSheets(config config.Config) (map[string]LWODSheet, error) {
	var lwod = make(map[string]LWODSheet, 0)

	resultYears, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, config.Folder)).Fields("files(*)").Do()
	if err != nil {
		log.Fatalf("Drive error: %v", err)
	}

	if !config.Flags.AllSheets {
		today := time.Now()
		oneMonthAgo := today.AddDate(0, -1, 0)
		plusSixDays := today.AddDate(0, 0, 6)

		for _, fileYears := range resultYears.Files {
			if fileYears.MimeType == "application/vnd.google-apps.folder" {
				switch fileYears.Name {
				case today.Format("2006"):
					result, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, fileYears.Id)).Fields("files(*)").Do()
					if err != nil {
						log.Fatalf("Drive error: %v", err)
					}
					for _, sheet := range result.Files {
						if sheet.MimeType == "application/vnd.google-apps.spreadsheet" {
							key := ""
							switch sheet.Name[:2] {
							case today.Format("01"):
								key = "Today"
							case oneMonthAgo.Format("01"):
								key = "OneMonthAgo"
							case plusSixDays.Format("01"):
								key = "PlusSixDays"
							}
							if key != "" {
								lwod[key] = LWODSheet{
									ID:   sheet.Id,
									Name: sheet.Name,
								}
							}
						}
					}
				case oneMonthAgo.Format("2006"):
					result, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, fileYears.Id)).Fields("files(*)").Do()
					if err != nil {
						log.Fatalf("Drive error: %v", err)
					}
					for _, sheet := range result.Files {
						if sheet.MimeType == "application/vnd.google-apps.spreadsheet" && sheet.Name[:2] == oneMonthAgo.Format("01") {
							lwod["OneMonthAgo"] = LWODSheet{
								ID:   sheet.Id,
								Name: sheet.Name,
							}
						}
					}
				case plusSixDays.Format("2006"):
					result, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, fileYears.Id)).Fields("files(*)").Do()
					if err != nil {
						log.Fatalf("Drive error: %v", err)
					}
					for _, sheet := range result.Files {
						if sheet.MimeType == "application/vnd.google-apps.spreadsheet" && sheet.Name[:2] == plusSixDays.Format("01") {
							lwod["PlusSixDays"] = LWODSheet{
								ID:   sheet.Id,
								Name: sheet.Name,
							}
						}
					}
				}
			}
		}
	} else {
		for _, fileYears := range resultYears.Files {
			if fileYears.MimeType == "application/vnd.google-apps.folder" {
				result, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, fileYears.Id)).Fields("files(*)").Do()
				if err != nil {
					log.Fatalf("Drive error: %v", err)
				}
				for _, sheet := range result.Files {
					if sheet.MimeType == "application/vnd.google-apps.spreadsheet" {
						lwod[fmt.Sprintf(`%s - %s`, fileYears.Name, sheet.Name[:2])] = LWODSheet{
							ID:   sheet.Id,
							Name: sheet.Name,
						}
					}
				}
			}
		}
	}

	return lwod, nil
}

func ParseSheets(sheets map[string]LWODSheet, config config.Config) {
	numberRegex := regexp.MustCompile(`(\d+)`)
	youtubeTimeRegex := regexp.MustCompile(`(?P<hours>\d+)h(?P<minutes>\d+)m(?P<seconds>\d+)s|(?P<onlysec>^\d+$)`)

	y := 0
	for sheetKey, sheet := range sheets {
		log.Infof(`Running sheet ID %s (name: "%s", number %d/%d)`, sheet.ID, sheet.Name, y+1, len(sheets))
		file, err := config.GoogleConfig.Sheets.FetchSpreadsheet(sheet.ID)
		if err != nil {
			log.Fatalf("Sheets error: %v", err)
		}
		for k, ws := range file.Sheets {
			log.Infof(`Running worksheet number %d/%d (name: "%s")`, k+1, len(file.Sheets), ws.Properties.Title)
			firstRow := getRowValues(ws.Rows[0])
			if slices.Contains(firstRow, "Topic") && slices.Contains(firstRow, "Date") {
				template := createTemplate(firstRow)
				log.Debugf("Created the template for current worksheet: %+v", template)

				entries := make(map[string][]LWODEntry)
				hashes := make(map[string]string)
				ytURLs := make(map[string][]LWODEntry)
				twitchURLs := make(map[string][]LWODEntry)

				dates := make(map[int]time.Time)
				var timeBuffer time.Time
				// for i, v := range ws.Rows {

				// }

				for i, v := range ws.Rows {
					var youtubeID string
					var youtubeStamp int
					var twitchID string
					if strings.Contains(v[template.Date].Value, "/") {
						timeBuffer, err = time.Parse("02/01/06", v[template.Date].Value)
						if err != nil {
							timeBuffer, err = time.Parse("01/02/06", v[template.Date].Value)
							if err != nil {
								log.Fatalf("Time parse error: %v", err)
							}
						}
					}
					dates[i] = timeBuffer
					if strings.Contains(v[template.YouTube].Value, "youtu") {
						ytURL, err := url.Parse(strings.TrimSpace(v[template.YouTube].Value))
						if err != nil {
							log.Fatalf("URL parse error: %v", err)
						}
						switch ytURL.Host {
						case "youtu.be":
							youtubeID = ytURL.Path[1:]
						case "youtube.com":
							youtubeID = ytURL.Query().Get("v")
						default:
							log.Debugf("No YouTube URL in row: %+v", v[template.YouTube].Value)
						}
						matches := youtubeTimeRegex.FindStringSubmatch(ytURL.Query().Get("t"))
						if len(matches) > 0 {
							onlySecIndex := youtubeTimeRegex.SubexpIndex("onlysec")
							hoursIndex := youtubeTimeRegex.SubexpIndex("hours")
							minutesIndex := youtubeTimeRegex.SubexpIndex("minutes")
							secondsIndex := youtubeTimeRegex.SubexpIndex("seconds")
							if matches[onlySecIndex] != "" {
								youtubeStamp, err = strconv.Atoi(matches[onlySecIndex])
								if err != nil {
									log.Fatalf("strconv error: %v", err)
								}
							} else {
								hours, err := strconv.Atoi(matches[hoursIndex])
								if err != nil {
									log.Fatalf("strconv error: %v", err)
								}
								minutes, err := strconv.Atoi(matches[minutesIndex])
								if err != nil {
									log.Fatalf("strconv error: %v", err)
								}
								seconds, err := strconv.Atoi(matches[secondsIndex])
								if err != nil {
									log.Fatalf("strconv error: %v", err)
								}
								youtubeStamp = (hours * 60 * 60) + (minutes * 60) + seconds
							}
						}
					}
					if strings.Contains(v[template.Twitch].Value, "twitch.tv/videos") {
						twitchURL, err := url.Parse(strings.TrimSpace(v[template.Twitch].Value))
						if err != nil {
							log.Fatalf("URL parse error: %v", err)
						}
						id := numberRegex.FindString(twitchURL.Path)
						if id != "" {
							twitchID = id
						} else {
							log.Debugf("No Twitch URL in row: %+v", v[template.Twitch].Value)
						}
					}
					if youtubeID != "" || twitchID != "" {
						entry := LWODEntry{
							DateAdded:    time.Now().UTC(),
							DateStreamed: dates[i],
							YouTube:      youtubeID,
							Twitch:       twitchID,
							Start:        v[template.Start].Value,
							End:          v[template.End].Value,
							YouTubeStamp: youtubeStamp,
							Game:         v[template.Game].Value,
							Subject:      v[template.Subject].Value,
							Topic:        v[template.Topic].Value,
						}
						if youtubeID != "" {
							ytURLs[youtubeID] = append(ytURLs[youtubeID], entry)
						}
						if twitchID != "" {
							twitchURLs[twitchID] = append(twitchURLs[twitchID], entry)
						}
					}
				}

				maps.Copy(entries, ytURLs)
				maps.Copy(entries, twitchURLs)

				for key, dataSlice := range ytURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.YouTube + value.Twitch + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.DBConfig.Statements.SelectYTHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("Couldn't find a row with YouTube ID %s, adding it to the DB.", key)
						default:
							log.Fatalf("Sqlite error (YouTube ID %s): %v", key, err)
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("For YouTube ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.DBConfig.Statements.DeleteYTStmt.Exec(key)
						if err != nil {
							log.Fatalf("Couldn't delete entries with YouTube ID %s: %v", key, err)
						}
						_, err = config.DBConfig.Statements.InsertYTStmt.Exec(key, hashNew)
						if err != nil {
							log.Fatalf("Couldn't insert entry with YouTube ID %s: %v", key, err)
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.DBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								log.Fatalf("Couldn't insert entry into lwodUrl with YouTube ID %s: %v", key, err)
							}
						}
					}
				}
				for key, dataSlice := range twitchURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.YouTube + value.Twitch + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.DBConfig.Statements.SelectTwitchHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("Couldn't find a row with Twitch ID %s, adding it to the DB", key)
						default:
							log.Fatalf("Sqlite error (Twitch ID %s): %v", key, err)
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("For Twitch ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.DBConfig.Statements.DeleteTwitchStmt.Exec(key)
						if err != nil {
							log.Fatalf("Couldn't delete entries with Twitch ID %s: %v", key, err)
						}
						_, err = config.DBConfig.Statements.InsertTwitchStmt.Exec(key, hashNew)
						if err != nil {
							log.Fatalf("Couldn't insert entry with Twitch ID %s: %v", key, err)
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.DBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								log.Fatalf("Couldn't insert entry into lwodUrl with Twitch ID %s: %v", key, err)
							}
						}
					}
				}

				dedupedEntries := dedupHashes(hashes, entries)
				log.Debugf("Deduped entries: %d", len(dedupedEntries))
				for _, value := range dedupedEntries {
					tx, err := config.DBConfig.DB.Begin()
					if err != nil {
						log.Fatalf(`Couldn't begin the Tx (spreadsheet %s: "%s", worksheet %d: "%s"): %v`, sheet.ID, sheet.Name, k+1, ws.Properties.Title, err)
					}
					for _, entry := range value {
						_, err = tx.Exec(
							"INSERT INTO lwod (dateadded, datestreamed, vodid, vidid, starttime, endtime, yttime, game, subject, topic) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
							entry.DateAdded,
							entry.DateStreamed,
							newNullString(entry.Twitch),
							newNullString(entry.YouTube),
							entry.Start,
							entry.End,
							entry.YouTubeStamp,
							entry.Game,
							entry.Subject,
							entry.Topic,
						)
						if err != nil {
							tx.Rollback()
							log.Fatalf("Couldn't insert entry %+v: %v", entry, err)
						}
					}
					tx.Commit()
				}
			}
			if k != len(file.Sheets)-1 {
				time.Sleep(time.Second * time.Duration(config.Delay))
			}
		}
		if y != len(sheets)-1 {
			time.Sleep(time.Second * time.Duration(config.Delay))
		}
		y++
	}
}
