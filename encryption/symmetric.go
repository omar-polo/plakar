package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"golang.org/x/crypto/scrypt"
)

type Configuration struct {
	Algorithm string
	Key       string
}

const (
	saltSize  = 16
	chunkSize = 1024 // Size of each chunk for encryption/decryption
)

func DefaultConfiguration() *Configuration {
	return &Configuration{
		Algorithm: "AES256-GCM",
	}
}

// BuildSecretFromPassphrase generates a secret from a passphrase using scrypt
func BuildSecretFromPassphrase(passphrase []byte) (string, error) {
	// Generate a random salt
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}

	// Derive the key using scrypt with high CPU and memory costs
	dk, err := scrypt.Key(passphrase, salt, 1<<15, 8, 1, 32)
	if err != nil {
		return "", fmt.Errorf("key derivation failed: %w", err)
	}

	// Return the base64-encoded secret including the salt
	return base64.StdEncoding.EncodeToString(append(salt, dk...)), nil
}

// DeriveSecret derives a secret key from a passphrase and a stored secret using scrypt
func DeriveSecret(passphrase []byte, secret string) ([]byte, error) {
	decodedSecret, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, err
	}

	salt := decodedSecret[:saltSize]
	expectedKey := decodedSecret[saltSize:]

	// Derive the key using scrypt with the same parameters
	dk, err := scrypt.Key(passphrase, salt, 1<<15, 8, 1, 32)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(dk, expectedKey) {
		return nil, fmt.Errorf("passphrase does not match")
	}
	return dk, nil
}

// EncryptStream encrypts a stream using AES-GCM with a random session-specific subkey
func EncryptStream(key []byte, r io.Reader) (io.Reader, error) {
	// Generate a random subkey for data encryption
	subkey := make([]byte, 32)
	if _, err := rand.Read(subkey); err != nil {
		return nil, err
	}

	// Encrypt the subkey with the main key using AES-GCM
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Generate a nonce for subkey encryption
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(subkeyNonce); err != nil {
		return nil, err
	}

	// Encrypt the subkey
	encSubkey := gcm.Seal(nil, subkeyNonce, subkey, nil)

	// Set up AES-GCM for data encryption using the subkey
	dataBlock, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	dataGCM, err := cipher.NewGCM(dataBlock)
	if err != nil {
		return nil, err
	}

	// Generate a nonce for data encryption
	dataNonce := make([]byte, dataGCM.NonceSize())
	if _, err := rand.Read(dataNonce); err != nil {
		return nil, err
	}

	// Set up the pipe for streaming encryption
	pr, pw := io.Pipe()

	// Start encryption in a goroutine
	go func() {
		defer pw.Close()
		// Write the encrypted subkey and both nonces to the output stream
		pw.Write(subkeyNonce)
		pw.Write(encSubkey)
		pw.Write(dataNonce)

		// Encrypt and write the actual data in chunks
		buf := make([]byte, chunkSize)
		for {
			n, err := r.Read(buf)
			if err != nil {
				if err != io.EOF {
					pw.CloseWithError(err)
				}
				break
			}
			// Encrypt each chunk and write it to the pipe
			encryptedChunk := dataGCM.Seal(nil, dataNonce, buf[:n], nil)
			if _, err := pw.Write(encryptedChunk); err != nil {
				pw.CloseWithError(err)
				break
			}
		}
	}()

	return pr, nil
}

// DecryptStream decrypts a stream using AES-GCM with a random session-specific subkey
func DecryptStream(key []byte, r io.Reader) (io.Reader, error) {
	// Set up to decrypt the subkey from the input
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Read and decrypt the subkey
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(r, subkeyNonce); err != nil {
		return nil, err
	}

	encSubkey := make([]byte, gcm.Overhead()+32) // GCM overhead for the 32-byte subkey
	if _, err := io.ReadFull(r, encSubkey); err != nil {
		return nil, err
	}

	subkey, err := gcm.Open(nil, subkeyNonce, encSubkey, nil)
	if err != nil {
		return nil, err
	}

	// Set up AES-GCM for actual data decryption using the subkey
	dataBlock, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	dataGCM, err := cipher.NewGCM(dataBlock)
	if err != nil {
		return nil, err
	}

	// Read the data nonce from the input
	dataNonce := make([]byte, dataGCM.NonceSize())
	if _, err := io.ReadFull(r, dataNonce); err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	// Start decryption in a goroutine
	go func() {
		defer pw.Close()

		// Decrypt the data in chunks and write it to the pipe
		buf := make([]byte, chunkSize+dataGCM.Overhead())
		for {
			n, err := r.Read(buf)
			if err != nil {
				if err != io.EOF {
					pw.CloseWithError(err)
				}
				break
			}
			// Decrypt each chunk and write it to the pipe
			decryptedChunk, err := dataGCM.Open(nil, dataNonce, buf[:n], nil)
			if err != nil {
				pw.CloseWithError(err)
				break
			}
			if _, err := pw.Write(decryptedChunk); err != nil {
				pw.CloseWithError(err)
				break
			}
		}
	}()

	return pr, nil
}
