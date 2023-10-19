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

package s3

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"

	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/PlakarLabs/plakar/cache"
)

type S3Repository struct {
	config storage.RepositoryConfig

	Cache *cache.Cache

	encoder *gob.Encoder
	decoder *gob.Decoder
	mu      sync.Mutex

	Repository string

	inflightRequests map[uuid.UUID]chan network.Request
	//registerInflight     chan inflight
	notifications chan network.Request
	//maxConcurrentRequest chan bool

	minioClient *minio.Client
	bucketName  string
}

type S3Transaction struct {
	Uuid       uuid.UUID
	repository *S3Repository
}

func init() {
	network.ProtocolRegister()
	storage.Register("s3", NewS3Repository)
}

func NewS3Repository() storage.RepositoryBackend {
	return &S3Repository{}
}

func (repository *S3Repository) connect(location *url.URL) error {
	endpoint := location.Host
	accessKeyID := location.User.Username()
	secretAccessKey, _ := location.User.Password()
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	repository.minioClient = minioClient
	return nil
}

func (repository *S3Repository) Create(location string, config storage.RepositoryConfig) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}
	repository.bucketName = parsed.RequestURI()[1:]

	err = repository.minioClient.MakeBucket(context.Background(), repository.bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	jconfig, err := msgpack.Marshal(config)
	if err != nil {
		return err
	}

	_, err = repository.minioClient.PutObject(context.Background(), repository.bucketName, "CONFIG", bytes.NewReader(jconfig), int64(len(jconfig)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	repository.config = config
	return nil
}

func (repository *S3Repository) Open(location string) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}

	repository.bucketName = parsed.RequestURI()[1:]

	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket does not exist")
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, "CONFIG", minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	stat, err := object.Stat()
	if err != nil {
		return err
	}

	configBytes := make([]byte, stat.Size)
	_, err = object.Read(configBytes)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}
	object.Close()

	var config storage.RepositoryConfig
	err = msgpack.Unmarshal(configBytes, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *S3Repository) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *S3Repository) Transaction(indexID uuid.UUID) (storage.TransactionBackend, error) {
	tx := &S3Transaction{}
	tx.Uuid = indexID
	tx.repository = repository
	return tx, nil
}

func (repository *S3Repository) GetSnapshots() ([]uuid.UUID, error) {
	ret := make([]uuid.UUID, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "SNAPSHOT/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "SNAPSHOT/") && len(object.Key) >= 12 {
			ret = append(ret, uuid.MustParse(object.Key[12:]))
		}
	}
	return ret, nil
}

func (repository *S3Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("SNAPSHOT/%s/%s", indexID.String()[0:2], indexID.String()), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("SNAPSHOT/%s/%s", indexID.String()[0:2], indexID.String()), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}

func (repository *S3Repository) GetBlobs() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "BLOB/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "BLOB/") && len(object.Key) >= 8 {
			t, err := hex.DecodeString(object.Key[8:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 [32]byte
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *S3Repository) PutBlob(checksum [32]byte, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("BLOB/%02x/%016x", checksum[0], checksum), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("BLOB/%02x/%016x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}

func (repository *S3Repository) DeleteBlob(checksum [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("BLOB/%02x/%016x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) GetChunks() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{}) {
		if strings.HasPrefix(object.Key, "CHUNK:") {
			var key [32]byte
			copy(key[:], object.Key[6:])
			ret = append(ret, key)
		}
	}
	return ret, nil
}

func (repository *S3Repository) GetObjects() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{}) {
		if strings.HasPrefix(object.Key, "OBJECT:") {
			var key [32]byte
			copy(key[:], object.Key[7:])
			ret = append(ret, key)
		}
	}
	return ret, nil
}

/*
func (repository *S3Repository) GetObject(checksum [32]byte) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("OBJECT/%02x/%064x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}
*/

func (repository *S3Repository) GetChunk(checksum [32]byte) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("CHUNK/%02x/%064x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}

func (repository *S3Repository) CheckObject(checksum [32]byte) (bool, error) {
	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("OBJECT/%02x/%064x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return false, err
	}
	_, err = object.Stat()
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (repository *S3Repository) CheckChunk(checksum [32]byte) (bool, error) {
	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("CHUNK/%02x/%064x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return false, err
	}
	_, err = object.Stat()
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (repository *S3Repository) PutObject(checksum [32]byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("OBJECT/%02x/%064x", checksum[0], checksum), bytes.NewReader([]byte("")), int64(0), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) DeleteObject(checksum [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("OBJECT/%02x/%064x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) PutChunk(checksum [32]byte, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("CHUNK/%02x/%064x", checksum[0], checksum), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) DeleteChunk(checksum [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("CHUNK/%02x/%064x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) DeleteSnapshot(indexID uuid.UUID) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("SNAPSHOT/%s/%s", indexID.String()[0:2], indexID.String()), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *S3Repository) Close() error {
	return nil
}

//////

func (transaction *S3Transaction) GetUuid() uuid.UUID {
	return transaction.Uuid
}

func (transaction *S3Transaction) Commit(data []byte) error {
	repository := transaction.repository
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("SNAPSHOT/%s/%s", transaction.Uuid.String()[0:2], transaction.Uuid.String()), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}