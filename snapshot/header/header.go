package header

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/profiler"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Importer struct {
	Type      string
	Origin    string
	Directory string
}

type Identity struct {
	Identifier uuid.UUID
	PublicKey  []byte
}

type KeyValue struct {
	Key   string
	Value string
}

type Header struct {
	SnapshotID       objects.Checksum
	Version          string
	CreationTime     time.Time
	CreationDuration time.Duration

	Identity Identity

	Category string
	Tags     []string

	Context []KeyValue

	Importer Importer

	Root       objects.Checksum
	Index      objects.Checksum
	Metadata   objects.Checksum
	Statistics objects.Checksum
	Errors     objects.Checksum

	Summary vfs.Summary
}

func NewHeader(indexID [32]byte) *Header {
	return &Header{
		SnapshotID:   indexID,
		CreationTime: time.Now(),
		Version:      storage.VERSION,
		Category:     "default",
		Tags:         []string{},

		Identity: Identity{},

		Importer: Importer{},

		Context: make([]KeyValue, 0),

		Root:       [32]byte{},
		Index:      [32]byte{},
		Metadata:   [32]byte{},
		Statistics: [32]byte{},
		Errors:     [32]byte{},
	}
}

func NewFromBytes(serialized []byte) (*Header, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("header.NewIndexFromBytes", time.Since(t0))
		logger.Trace("header", "NewMetadataFromBytes(...): %s", time.Since(t0))
	}()

	var header Header
	if err := msgpack.Unmarshal(serialized, &header); err != nil {
		return nil, err
	} else {
		return &header, nil
	}
}

func (h *Header) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("header.Serialize", time.Since(t0))
		logger.Trace("header", "Serialize(): %s", time.Since(t0))
	}()

	if serialized, err := msgpack.Marshal(h); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func (h *Header) SetContext(key, value string) {
	h.Context = append(h.Context, KeyValue{Key: key, Value: value})
}

func (h *Header) GetContext(key string) string {
	for _, kv := range h.Context {
		if kv.Key == key {
			return kv.Value
		}
	}
	return ""
}

func (h *Header) GetIndexID() [32]byte {
	return h.SnapshotID
}

func (h *Header) GetIndexShortID() []byte {
	return h.SnapshotID[:4]
}

func (h *Header) GetRoot() [32]byte {
	return h.Root
}

func ParseSortKeys(sortKeysStr string) ([]string, error) {
	if sortKeysStr == "" {
		return nil, nil
	}
	keys := strings.Split(sortKeysStr, ",")
	uniqueKeys := make(map[string]bool)
	validKeys := []string{}

	headerType := reflect.TypeOf(Header{})

	for _, key := range keys {
		key = strings.TrimSpace(key)
		lookupKey := key
		if strings.HasPrefix(key, "-") {
			lookupKey = key[1:]
		}
		if uniqueKeys[lookupKey] {
			return nil, errors.New("duplicate sort key: " + key)
		}
		uniqueKeys[lookupKey] = true

		if _, found := headerType.FieldByName(lookupKey); !found {
			return nil, errors.New("invalid sort key: " + key)
		}
		validKeys = append(validKeys, key)
	}

	return validKeys, nil
}

func SortHeaders(headers []Header, sortKeys []string) error {
	var err error
	sort.Slice(headers, func(i, j int) bool {
		for _, key := range sortKeys {
			switch key {
			case "CreationTime":
				if !headers[i].CreationTime.Equal(headers[j].CreationTime) {
					return headers[i].CreationTime.Before(headers[j].CreationTime)
				}
			case "-CreationTime":
				if !headers[i].CreationTime.Equal(headers[j].CreationTime) {
					return headers[i].CreationTime.After(headers[j].CreationTime)
				}
			case "SnapshotID":
				for k := 0; k < len(headers[i].SnapshotID); k++ {
					if headers[i].SnapshotID[k] != headers[j].SnapshotID[k] {
						return headers[i].SnapshotID[k] < headers[j].SnapshotID[k]
					}
				}
			case "-SnapshotID":
				for k := 0; k < len(headers[i].SnapshotID); k++ {
					if headers[i].SnapshotID[k] != headers[j].SnapshotID[k] {
						return headers[i].SnapshotID[k] > headers[j].SnapshotID[k]
					}
				}
			case "Version":
				if headers[i].Version != headers[j].Version {
					return headers[i].Version < headers[j].Version
				}
			case "-Version":
				if headers[i].Version != headers[j].Version {
					return headers[i].Version > headers[j].Version
				}

			case "Tags":
				// Compare Tags lexicographically, element by element
				for k := 0; k < len(headers[i].Tags) && k < len(headers[j].Tags); k++ {
					if headers[i].Tags[k] != headers[j].Tags[k] {
						return headers[i].Tags[k] < headers[j].Tags[k]
					}
				}
				if len(headers[i].Tags) != len(headers[j].Tags) {
					return len(headers[i].Tags) < len(headers[j].Tags)
				}
			case "-Tags":
				// Compare Tags lexicographically, element by element
				for k := 0; k < len(headers[i].Tags) && k < len(headers[j].Tags); k++ {
					if headers[i].Tags[k] != headers[j].Tags[k] {
						return headers[i].Tags[k] > headers[j].Tags[k]
					}
				}
				if len(headers[i].Tags) != len(headers[j].Tags) {
					return len(headers[i].Tags) > len(headers[j].Tags)
				}
			default:
				err = errors.New("invalid sort key: " + key)
				return false
			}
		}
		return false
	})
	return err
}
