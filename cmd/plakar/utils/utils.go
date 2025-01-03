/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package utils

import (
	"encoding/hex"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"golang.org/x/mod/semver"
	"golang.org/x/term"
	"golang.org/x/tools/blog/atom"
)

func ParseSnapshotID(id string) (string, string) {
	tmp := strings.Split(id, ":")
	prefix := id
	pattern := ""
	if len(tmp) != 0 {
		prefix = tmp[0]
		pattern = strings.Join(tmp[1:], ":")
		if runtime.GOOS != "windows" {
			if !strings.HasPrefix(pattern, "/") {
				pattern = "/" + pattern
			}
		}
	}
	return prefix, pattern
}

func LookupSnapshotByPrefix(repo *repository.Repository, prefix string) []objects.Checksum {
	ret := make([]objects.Checksum, 0)
	for snapshotID := range repo.ListSnapshots() {
		if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), prefix) {
			ret = append(ret, snapshotID)
		}
	}
	return ret
}

func LocateSnapshotByPrefix(repo *repository.Repository, prefix string) (objects.Checksum, error) {
	snapshots := LookupSnapshotByPrefix(repo, prefix)
	if len(snapshots) == 0 {
		return objects.Checksum{}, fmt.Errorf("no snapshot has prefix: %s", prefix)
	}
	if len(snapshots) > 1 {
		return objects.Checksum{}, fmt.Errorf("snapshot ID is ambiguous: %s (matches %d snapshots)", prefix, len(snapshots))
	}
	return snapshots[0], nil
}

func OpenSnapshotByPrefix(repo *repository.Repository, prefix string) (*snapshot.Snapshot, error) {
	snapshotID, err := LocateSnapshotByPrefix(repo, prefix)
	if err != nil {
		return nil, err
	}
	return snapshot.Load(repo, snapshotID)
}

func GetSnapshotsList(repo *repository.Repository) ([]objects.Checksum, error) {
	snapshots, err := repo.GetSnapshots()
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func GetHeaders(repo *repository.Repository, prefixes []string) ([]*header.Header, error) {
	snapshotsList, err := GetSnapshotsList(repo)
	if err != nil {
		return nil, err
	}

	result := make([]*header.Header, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotID := range snapshotsList {
			wg.Add(1)
			go func(snapshotID objects.Checksum) {
				defer wg.Done()
				hdr, _, err := snapshot.GetSnapshot(repo, snapshotID)
				if err != nil {
					fmt.Println(err)
					return
				}
				mu.Lock()
				result = append(result, hdr)
				mu.Unlock()
			}(snapshotID)
		}
		wg.Wait()
		sort.Slice(result, func(i, j int) bool {
			return result[i].CreationTime.Before(result[j].CreationTime)
		})
		return result, nil
	}

	tags := make(map[string]objects.Checksum)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotID := range snapshotsList {
		hdr, _, err := snapshot.GetSnapshot(repo, snapshotID)
		if err != nil {
			return nil, err
		}
		for _, tag := range hdr.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotID
				tagsTimestamp[tag] = hdr.CreationTime
			} else if recordTime.Before(hdr.CreationTime) {
				tags[tag] = snapshotID
				tagsTimestamp[tag] = hdr.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := ParseSnapshotID(prefix)

		matches := 0
		for _, snapshotID := range snapshotsList {
			if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotID := range snapshotsList {
			if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), parsedUuidPrefix) || snapshotID == tags[parsedUuidPrefix] {
				metadata, _, err := snapshot.GetSnapshot(repo, snapshotID)
				if err != nil {
					return nil, err
				}
				result = append(result, metadata)
			}
		}
	}
	return result, nil
}

func GetSnapshots(repo *repository.Repository, prefixes []string) ([]*snapshot.Snapshot, error) {
	snapshotsList, err := GetSnapshotsList(repo)
	if err != nil {
		return nil, err
	}

	result := make([]*snapshot.Snapshot, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotID := range snapshotsList {
			wg.Add(1)
			go func(snapshotID objects.Checksum) {
				defer wg.Done()
				snapshotInstance, err := snapshot.Load(repo, snapshotID)
				if err != nil {
					return
				}
				mu.Lock()
				result = append(result, snapshotInstance)
				mu.Unlock()
			}(snapshotID)
		}
		wg.Wait()
		return sortSnapshotsByDate(result), nil
	}

	tags := make(map[string]objects.Checksum)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotID := range snapshotsList {
		metadata, _, err := snapshot.GetSnapshot(repo, snapshotID)
		if err != nil {
			return nil, err
		}
		for _, tag := range metadata.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotID
				tagsTimestamp[tag] = metadata.CreationTime
			} else if recordTime.Before(metadata.CreationTime) {
				tags[tag] = snapshotID
				tagsTimestamp[tag] = metadata.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := ParseSnapshotID(prefix)

		matches := 0
		for _, snapshotID := range snapshotsList {
			if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotID := range snapshotsList {
			if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), parsedUuidPrefix) || snapshotID == tags[parsedUuidPrefix] {
				snapshotInstance, err := snapshot.Load(repo, snapshotID)
				if err != nil {
					return nil, err
				}
				result = append(result, snapshotInstance)
			}
		}
	}
	return result, nil
}

func sortSnapshotsByDate(snapshots []*snapshot.Snapshot) []*snapshot.Snapshot {
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Header.CreationTime.Before(snapshots[j].Header.CreationTime)
	})
	return snapshots
}

func HumanToDuration(human string) (time.Duration, error) {
	// support either one of the following:
	// - time.Duration string
	// - human readable string (e.g. 1h, 1d, 1w, 1m, 1y)
	// - human readable string with time.Duration suffix (e.g. 1h30m, 1d12h, 1w3d, 1m2w, 1y1m)

	// first we check if it's a time.Duration string
	duration, err := time.ParseDuration(human)
	if err == nil {
		return duration, nil
	}

	// TODO-handle iteratively constructed human readable strings

	return 0, fmt.Errorf("invalid duration: %s", human)
}

type ReleaseUpdateSummary struct {
	FoundCount int
	Latest     string

	SecurityFix    bool
	ReliabilityFix bool
}

func CheckUpdate() (ReleaseUpdateSummary, error) {
	req, err := http.NewRequest("GET", "https://plakar.io/api/releases.atom", nil)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}

	req.Header.Set("User-Agent", fmt.Sprintf("plakar/%s (%s/%s)", VERSION, runtime.GOOS, runtime.GOARCH))

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}
	defer res.Body.Close()

	var feed []atom.Feed
	err = xml.NewDecoder(res.Body).Decode(&feed)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}

	found := false
	foundCount := 0
	var latestEntry *atom.Entry
	sawSecurityFix := false
	sawReliabilityFix := false
	for _, entry := range feed[0].Entry {
		if !semver.IsValid(entry.Title) {
			continue
		}
		if semver.Compare(VERSION, entry.Title) < 0 {
			found = true
			foundCount++
			if latestEntry == nil {
				latestEntry = entry
			} else {
				if semver.Compare(latestEntry.Title, entry.Title) < 0 {
					latestEntry = entry
				}
			}
			if latestEntry.Content != nil {
				if strings.Contains(latestEntry.Content.Body, "SECURITY") {
					sawSecurityFix = true
				}
				if strings.Contains(latestEntry.Content.Body, "RELIABILITY") {
					sawReliabilityFix = true
				}
			}
		}
	}
	if !found {
		return ReleaseUpdateSummary{FoundCount: 0, Latest: VERSION}, nil
	} else {
		return ReleaseUpdateSummary{FoundCount: foundCount, Latest: latestEntry.Title, SecurityFix: sawSecurityFix, ReliabilityFix: sawReliabilityFix}, nil
	}
}

func PathIsWithin(pathname string, within string) bool {
	cleanPath := filepath.Clean(pathname)
	cleanWithin := filepath.Clean(within)

	if cleanWithin == "/" {
		return true
	}

	return strings.HasPrefix(cleanPath, cleanWithin+"/")
}

func GetPassphrase(prefix string) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}
	return passphrase, nil
}

func GetPassphraseConfirm(prefix string) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase1, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "%s passphrase (confirm): ", prefix)
	passphrase2, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	if string(passphrase1) != string(passphrase2) {
		return nil, errors.New("passphrases mismatch")
	}

	return passphrase1, nil
}

func GetCacheDir(appName string) (string, error) {
	var cacheDir string

	switch runtime.GOOS {
	case "windows":
		// Use %LocalAppData%
		cacheDir = os.Getenv("LocalAppData")
		if cacheDir == "" {
			return "", fmt.Errorf("LocalAppData environment variable not set")
		}
		cacheDir = filepath.Join(cacheDir, appName)
	default:
		// Use XDG_CACHE_HOME or default to ~/.cache
		cacheDir = os.Getenv("XDG_CACHE_HOME")
		if cacheDir == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			cacheDir = filepath.Join(homeDir, ".cache", appName)
		} else {
			cacheDir = filepath.Join(cacheDir, appName)
		}
	}

	// Create the cache directory if it doesn't exist
	err := os.MkdirAll(cacheDir, 0700)
	if err != nil {
		return "", err
	}

	return cacheDir, nil
}

const VERSION = "v0.4.24-alpha"

func GetVersion() string {
	return VERSION
}

func NormalizePath(path string) (string, error) {
	path = filepath.Clean(path)
	parts := strings.Split(path, string(filepath.Separator))[1:]

	if len(parts) == 0 || parts[0] == "" {
		return "", fmt.Errorf("invalid path")
	}

	var normalizedPath string
	// For Windows, start with the drive letter.
	if filepath.IsAbs(path) {
		normalizedPath = string(filepath.Separator)
	}

	for _, part := range parts {
		if part == "" {
			continue
		}

		dirEntries, err := os.ReadDir(normalizedPath)
		if err != nil {
			return "", err
		}

		matched := false
		for _, entry := range dirEntries {
			if strings.EqualFold(entry.Name(), part) {
				normalizedPath = filepath.Join(normalizedPath, entry.Name())
				matched = true
				break
			}
		}

		if !matched {
			return "", fmt.Errorf("path not found: %s", path)
		}
	}

	return normalizedPath, nil
}
