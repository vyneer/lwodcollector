package gsheets

import (
	"database/sql"
	"encoding/json"
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
	"github.com/vyneer/lwodcollector/util"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type LWODSheet struct {
	ID   string
	Name string
}

type LWODTemplate struct {
	Date, Start, End, Game, Subject, Topic, VOD int
}

type LWODEntry struct {
	DateAdded, DateStreamed                                                 time.Time
	Twitch, YouTube, Rumble, Kick, Odysee, Start, End, Game, Subject, Topic string
	TwitchStamp, YouTubeStamp, RumbleStamp, KickStamp, OdyseeStamp          int
}

type LWODSheetData struct {
	YouTubeLinks, TwitchLinks, RumbleLinks, KickLinks, OdyseeLinks map[string][]LWODEntry
}

func maxOfTemplate(template LWODTemplate) int64 {
	tempReflectType := reflect.TypeOf(LWODTemplate{})
	var max int64 = 0

	for _, v := range reflect.VisibleFields(tempReflectType) {
		for k := 0; k < tempReflectType.NumField(); k++ {
			val := reflect.ValueOf(&template).Elem().FieldByName(v.Name).Int()
			if max < val {
				max = val
			}
		}
	}

	return max
}

func createTemplate(row []string) LWODTemplate {
	var template LWODTemplate
	tempReflectType := reflect.TypeOf(LWODTemplate{})

	for i, v := range row {
		lc := strings.ToLower(v)
		for k := 0; k < tempReflectType.NumField(); k++ {
			name := strings.ToLower(tempReflectType.Field(k).Name)
			if strings.Contains(lc, name) {
				reflect.ValueOf(&template).Elem().FieldByName(tempReflectType.Field(k).Name).SetInt(int64(i))
			}
		}
	}

	return template
}

func CollectSheets(config *config.Config) (map[string]LWODSheet, error) {
	var lwod = make(map[string]LWODSheet, 0)

	resultYears, err := config.GoogleConfig.Drive.Files.List().Q(fmt.Sprintf(`"%s" in parents`, config.LWODFolder)).Fields("files(*)").Do()
	if err != nil {
		return nil, WrapWithLWODError(err, "Drive error")
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
						return nil, WrapWithLWODError(err, "Drive error")
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
						return nil, WrapWithLWODError(err, "Drive error")
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
						return nil, WrapWithLWODError(err, "Drive error")
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
					return nil, WrapWithLWODError(err, "Drive error")
				}
				for _, sheet := range result.Files {
					if sheet.MimeType == "application/vnd.google-apps.spreadsheet" {
						lwod[fmt.Sprintf(`%s-%s`, fileYears.Name, sheet.Name[:2])] = LWODSheet{
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

func ParseSheets(sheets map[string]LWODSheet, config *config.Config) error {
	numberRegex := regexp.MustCompile(`(\d+)`)
	youtubeTimeRegex := regexp.MustCompile(`(?P<hours>\d+)h(?P<minutes>\d+)m(?P<seconds>\d+)s|(?P<onlysec>^\d+$)`)

	y := 0
	for sheetKey, sheet := range sheets {
		log.Infof(`[LWOD] Running sheet ID %s (name: "%s", number %d/%d)`, sheet.ID, sheet.Name, y+1, len(sheets))
		file, err := config.GoogleConfig.Sheets.Spreadsheets.Get(sheet.ID).Fields("spreadsheetId,properties.title,sheets(properties,data.rowData.values(userEnteredValue,effectiveValue,formattedValue,note))").Do()
		if err != nil {
			return WrapWithLWODError(err, "Sheets error")
		}
		for k, ws := range file.Sheets {
			log.Infof(`[LWOD] Running worksheet number %d/%d (name: "%s")`, k+1, len(file.Sheets), ws.Properties.Title)
			firstRow := getRowValues(ws.Data[0].RowData[0].Values)
			if slices.Contains(firstRow, "Topic") && slices.Contains(firstRow, "Date") {
				template := createTemplate(firstRow)
				maxValueOfTemplate := maxOfTemplate(template)
				log.Debugf("[LWOD] Created the template for current worksheet: %+v", template)

				entries := make(map[string][]LWODEntry)
				hashes := make(map[string]string)
				ytURLs := make(map[string][]LWODEntry)
				twitchURLs := make(map[string][]LWODEntry)
				rumbleURLs := make(map[string][]LWODEntry)
				kickURLs := make(map[string][]LWODEntry)
				odyseeURLs := make(map[string][]LWODEntry)

				dates := make(map[int]time.Time)
				var timeBuffer time.Time

				for i, row := range ws.Data[0].RowData {
					fillWithBlank(&row.Values, maxValueOfTemplate)
					var youtubeID string
					var youtubeStamp int
					var twitchID string
					// var twitchStamp int
					var rumbleID string
					var rumbleStamp int
					var kickID string
					// var kickStamp int
					var odyseeID string
					var odyseeStamp int
					v := row.Values

					if strings.Contains(v[template.Date].FormattedValue, "/") {
						timeBuffer, err = time.Parse("02/01/06", v[template.Date].FormattedValue)
						if err != nil {
							timeBuffer, err = time.Parse("01/02/06", v[template.Date].FormattedValue)
							if err != nil {
								return WrapWithLWODError(err, "Time parse error")
							}
						}
					}
					dates[i] = timeBuffer
					if strings.Contains(v[template.VOD].FormattedValue, "youtu.be") || strings.Contains(v[template.VOD].FormattedValue, "youtube.com") {
						ytURL, err := url.Parse(strings.TrimSpace(v[template.VOD].FormattedValue))
						if err != nil {
							return WrapWithLWODError(err, "URL parse error")
						}
						switch ytURL.Host {
						case "youtu.be":
							youtubeID = ytURL.Path[1:]
						case "youtube.com":
							youtubeID = ytURL.Query().Get("v")
						default:
							log.Debugf("[LWOD] No YouTube URL in row: %+v", v[template.VOD].FormattedValue)
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
									return WrapWithLWODError(err, "strconv error")
								}
							} else {
								hours, err := strconv.Atoi(matches[hoursIndex])
								if err != nil {
									return WrapWithLWODError(err, "strconv error")
								}
								minutes, err := strconv.Atoi(matches[minutesIndex])
								if err != nil {
									return WrapWithLWODError(err, "strconv error")
								}
								seconds, err := strconv.Atoi(matches[secondsIndex])
								if err != nil {
									return WrapWithLWODError(err, "strconv error")
								}
								youtubeStamp = (hours * 60 * 60) + (minutes * 60) + seconds
							}
						}
					}
					if strings.Contains(v[template.VOD].FormattedValue, "twitch.tv/videos") {
						twitchURL, err := url.Parse(strings.TrimSpace(v[template.VOD].FormattedValue))
						if err != nil {
							return WrapWithLWODError(err, "URL parse error")
						}
						id := numberRegex.FindString(twitchURL.Path)
						if id != "" {
							twitchID = id
						} else {
							log.Debugf("[LWOD] No Twitch URL in row: %+v", v[template.VOD].FormattedValue)
						}
					}
					if strings.Contains(v[template.VOD].FormattedValue, "rumble.com/embed") {
						rumbleURL, err := url.Parse(strings.TrimSpace(v[template.VOD].FormattedValue))
						if err != nil {
							return WrapWithLWODError(err, "URL parse error")
						}
						id := strings.Split(rumbleURL.Path, "/")[2]
						if id != "" {
							rumbleID = id
							rT := rumbleURL.Query().Get("t")
							if len(rT) > 0 {
								rumbleStamp, err = strconv.Atoi(numberRegex.FindString(rT))
								if err != nil {
									return WrapWithLWODError(err, "URL parse error")
								}
							}
						} else {
							log.Debugf("[LWOD] No Rumble URL in row: %+v", v[template.VOD].FormattedValue)
						}
					}
					if strings.Contains(v[template.VOD].FormattedValue, "kick.com/video") {
						kickURL, err := url.Parse(strings.TrimSpace(v[template.VOD].FormattedValue))
						if err != nil {
							return WrapWithLWODError(err, "URL parse error")
						}
						id := strings.Split(kickURL.Path, "/")[2]
						if id != "" {
							kickID = id
						} else {
							log.Debugf("[LWOD] No Kick URL in row: %+v", v[template.VOD].FormattedValue)
						}
					}
					if strings.Contains(v[template.VOD].FormattedValue, "odysee.com") {
						odyseeURL, err := url.Parse(strings.TrimSpace(v[template.VOD].FormattedValue))
						if err != nil {
							return WrapWithLWODError(err, "URL parse error")
						}
						id := odyseeURL.Path
						if id != "" {
							odyseeID = id
							oT := odyseeURL.Query().Get("t")
							if len(oT) > 0 {
								odyseeStamp, err = strconv.Atoi(numberRegex.FindString(oT))
								if err != nil {
									return WrapWithLWODError(err, "URL parse error")
								}
							}
						} else {
							log.Debugf("[LWOD] No Odysee URL in row: %+v", v[template.VOD].FormattedValue)
						}
					}
					if youtubeID != "" || twitchID != "" || rumbleID != "" || kickID != "" || odyseeID != "" {
						entry := LWODEntry{
							DateAdded:    time.Now().UTC(),
							DateStreamed: dates[i],
							YouTube:      youtubeID,
							Twitch:       twitchID,
							Rumble:       rumbleID,
							Kick:         kickID,
							Odysee:       odyseeID,
							Start:        v[template.Start].FormattedValue,
							End:          v[template.End].FormattedValue,
							YouTubeStamp: youtubeStamp,
							RumbleStamp:  rumbleStamp,
							OdyseeStamp:  odyseeStamp,
							Game:         v[template.Game].FormattedValue,
							Subject:      v[template.Subject].FormattedValue,
							Topic:        v[template.Topic].FormattedValue,
						}
						if youtubeID != "" {
							ytURLs[youtubeID] = append(ytURLs[youtubeID], entry)
						}
						if twitchID != "" {
							twitchURLs[twitchID] = append(twitchURLs[twitchID], entry)
						}
						if rumbleID != "" {
							rumbleURLs[rumbleID] = append(rumbleURLs[rumbleID], entry)
						}
						if kickID != "" {
							kickURLs[kickID] = append(kickURLs[kickID], entry)
						}
						if odyseeID != "" {
							odyseeURLs[odyseeID] = append(odyseeURLs[odyseeID], entry)
						}
					}
				}

				maps.Copy(entries, ytURLs)
				maps.Copy(entries, twitchURLs)
				maps.Copy(entries, rumbleURLs)
				maps.Copy(entries, kickURLs)
				maps.Copy(entries, odyseeURLs)

				for key, dataSlice := range ytURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.YouTube + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.LWODDBConfig.Statements.SelectYTHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("[LWOD] Couldn't find a row with YouTube ID %s, adding it to the DB.", key)
						default:
							return WrapWithLWODError(err, fmt.Sprintf("Sqlite error (YouTube ID %s)", key))
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("[LWOD] For YouTube ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.LWODDBConfig.Statements.DeleteYTStmt.Exec(key)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't delete entries with YouTube ID %s", key))
						}
						_, err = config.LWODDBConfig.Statements.InsertYTStmt.Exec(key, hashNew)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry with YouTube ID %s", key))
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with YouTube ID %s", key))
							}
						} else if config.Flags.AllSheets {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", sheetKey), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with YouTube ID %s", key))
							}
						}
					}
				}
				for key, dataSlice := range twitchURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.Twitch + value.YouTube + value.Rumble + value.Kick + value.Odysee + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + strconv.Itoa(value.TwitchStamp) + strconv.Itoa(value.RumbleStamp) + strconv.Itoa(value.KickStamp) + strconv.Itoa(value.OdyseeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.LWODDBConfig.Statements.SelectTwitchHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("[LWOD] Couldn't find a row with Twitch ID %s, adding it to the DB", key)
						default:
							return WrapWithLWODError(err, fmt.Sprintf("Sqlite error (Twitch ID %s)", key))
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("[LWOD] For Twitch ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.LWODDBConfig.Statements.DeleteTwitchStmt.Exec(key)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't delete entries with Twitch ID %s", key))
						}
						_, err = config.LWODDBConfig.Statements.InsertTwitchStmt.Exec(key, hashNew)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry with Twitch ID %s", key))
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Twitch ID %s", key))
							}
						} else if config.Flags.AllSheets {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", sheetKey), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Twitch ID %s", key))
							}
						}
					}
				}
				for key, dataSlice := range rumbleURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.Twitch + value.YouTube + value.Rumble + value.Kick + value.Odysee + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + strconv.Itoa(value.TwitchStamp) + strconv.Itoa(value.RumbleStamp) + strconv.Itoa(value.KickStamp) + strconv.Itoa(value.OdyseeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.LWODDBConfig.Statements.SelectRumbleHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("[LWOD] Couldn't find a row with Rumble ID %s, adding it to the DB", key)
						default:
							return WrapWithLWODError(err, fmt.Sprintf("Sqlite error (Rumble ID %s)", key))
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("[LWOD] For Rumble ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.LWODDBConfig.Statements.DeleteRumbleStmt.Exec(key)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't delete entries with Rumble ID %s", key))
						}
						_, err = config.LWODDBConfig.Statements.InsertRumbleStmt.Exec(key, hashNew)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry with Rumble ID %s", key))
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Rumble ID %s", key))
							}
						} else if config.Flags.AllSheets {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", sheetKey), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Rumble ID %s", key))
							}
						}
					}
				}
				for key, dataSlice := range kickURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.Twitch + value.YouTube + value.Rumble + value.Kick + value.Odysee + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + strconv.Itoa(value.TwitchStamp) + strconv.Itoa(value.RumbleStamp) + strconv.Itoa(value.KickStamp) + strconv.Itoa(value.OdyseeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.LWODDBConfig.Statements.SelectKickHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("[LWOD] Couldn't find a row with Kick ID %s, adding it to the DB", key)
						default:
							return WrapWithLWODError(err, fmt.Sprintf("Sqlite error (Kick ID %s)", key))
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("[LWOD] For Kick ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.LWODDBConfig.Statements.DeleteKickStmt.Exec(key)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't delete entries with Kick ID %s", key))
						}
						_, err = config.LWODDBConfig.Statements.InsertKickStmt.Exec(key, hashNew)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry with Kick ID %s", key))
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Kick ID %s", key))
							}
						} else if config.Flags.AllSheets {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", sheetKey), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Kick ID %s", key))
							}
						}
					}
				}
				for key, dataSlice := range odyseeURLs {
					var hashString string
					var hashOld string
					for _, value := range dataSlice {
						hashString += value.Twitch + value.YouTube + value.Rumble + value.Kick + value.Odysee + value.Start + value.End + strconv.Itoa(value.YouTubeStamp) + strconv.Itoa(value.TwitchStamp) + strconv.Itoa(value.RumbleStamp) + strconv.Itoa(value.KickStamp) + strconv.Itoa(value.OdyseeStamp) + value.Game + value.Subject + value.Topic
					}
					hashNewUint64 := xxhash.Sum64String(hashString)
					hashNew := strconv.FormatUint(hashNewUint64, 10)
					err := config.LWODDBConfig.Statements.SelectOdyseeHashStmt.QueryRow(key).Scan(&hashOld)
					if err != nil {
						switch {
						case errors.Is(err, sql.ErrNoRows):
							log.Debugf("[LWOD] Couldn't find a row with Odysee ID %s, adding it to the DB", key)
						default:
							return WrapWithLWODError(err, fmt.Sprintf("Sqlite error (Odysee ID %s)", key))
						}
					}
					if hashOld != hashNew {
						if hashOld != "" {
							log.Debugf("[LWOD] For Odysee ID %s, the old hash (%s...) doesn't equal the new hash (%s...), proceeding", key, hashOld[8:], hashNew[8:])
						}
						_, err := config.LWODDBConfig.Statements.DeleteOdyseeStmt.Exec(key)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't delete entries with Odysee ID %s", key))
						}
						_, err = config.LWODDBConfig.Statements.InsertOdyseeStmt.Exec(key, hashNew)
						if err != nil {
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry with Odysee ID %s", key))
						}
						hashes[key] = hashNew
						if k+1 == 2 && sheetKey == "Today" {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", time.Now().Format("2006-01")), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Odysee ID %s", key))
							}
						} else if config.Flags.AllSheets {
							_, err = config.LWODDBConfig.Statements.InsertURLStmt.Exec(fmt.Sprintf("%s-01", sheetKey), sheet.ID)
							if err != nil {
								return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry into lwodUrl with Odysee ID %s", key))
							}
						}
					}
				}

				dedupedEntries := dedupHashes(hashes, entries)
				log.Debugf("[LWOD] Deduped entries: %d", len(dedupedEntries))
				for _, value := range dedupedEntries {
					tx, err := config.LWODDBConfig.DB.Begin()
					if err != nil {
						return WrapWithLWODError(err, fmt.Sprintf(`Couldn't begin the Tx (spreadsheet %s: "%s", worksheet %d: "%s")`, sheet.ID, sheet.Name, k+1, ws.Properties.Title))
					}
					for _, entry := range value {
						_, err = tx.Exec(
							"INSERT INTO lwod (dateadded, datestreamed, vodid, vidid, rumbleid, kickid, odyseeid, starttime, endtime, yttime, twitchtime, rumbletime, kicktime, odyseetime, game, subject, topic) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
							entry.DateAdded,
							entry.DateStreamed,
							newNullString(entry.Twitch),
							newNullString(entry.YouTube),
							newNullString(entry.Rumble),
							newNullString(entry.Kick),
							newNullString(entry.Odysee),
							entry.Start,
							entry.End,
							entry.YouTubeStamp,
							entry.TwitchStamp,
							entry.RumbleStamp,
							entry.KickStamp,
							entry.OdyseeStamp,
							entry.Game,
							entry.Subject,
							entry.Topic,
						)
						if err != nil {
							tx.Rollback()
							return WrapWithLWODError(err, fmt.Sprintf("Couldn't insert entry %+v", entry))
						}
					}
					tx.Commit()
				}
			}
			if k != len(file.Sheets)-1 {
				time.Sleep(time.Second * time.Duration(config.LWODDelay))
			}
		}
		if y != len(sheets)-1 {
			time.Sleep(time.Second * time.Duration(config.LWODDelay))
		}
		y++
	}
	return nil
}

func SheetsLoop(cfg *config.Config) error {
	sheets, err := CollectSheets(cfg)
	if err != nil {
		return err
	}
	_, okToday := sheets["Today"]
	_, okOneMonthAgo := sheets["OneMonthAgo"]
	_, okPlusSixDays := sheets["PlusSixDays"]
	if okToday || okOneMonthAgo || okPlusSixDays {
		sheetsPretty, err := json.MarshalIndent(sheets, "", "	")
		if err != nil {
			return err
		}
		log.Infof("[LWOD] Grabbed the sheets from the folder: %+v", string(sheetsPretty))
	} else {
		log.Infof("[LWOD] Grabbed the sheets from the folder: %+v", sheets)
	}
	err = ParseSheets(sheets, cfg)
	if err != nil {
		return err
	}
	if cfg.LWODHealthCheck != "" && cfg.Continuous {
		util.HealthCheck(&cfg.LWODHealthCheck)
	}
	return nil
}
