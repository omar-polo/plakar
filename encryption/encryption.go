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

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/compression"
	"golang.org/x/crypto/pbkdf2"
)

func Keygen() (*Keypair, error) {
	pubkeyCurve := elliptic.P384() //see http://golang.org/pkg/crypto/elliptic/#P256

	privateKey, err := ecdsa.GenerateKey(pubkeyCurve, rand.Reader) // this generates a public & private key pair
	if err != nil {
		return nil, err
	}

	keypair := &Keypair{}
	keypair.CreationTime = time.Now()
	keypair.Uuid = uuid.NewString()
	keypair.PrivateKey = privateKey
	keypair.PublicKey = &privateKey.PublicKey
	keypair.MasterKey = make([]byte, 32)
	rand.Read(keypair.MasterKey)

	return keypair, nil
}

func Keyload(passphrase []byte, data []byte) (*Keypair, error) {
	keypair := &Keypair{}
	data, err := keypair.Decrypt(passphrase, data)
	if err != nil {
		return nil, err
	}

	return keypair.Deserialize(data)
}

func (keypair *Keypair) Serialize() (*SerializedKeypair, error) {
	x509priv, err := x509.MarshalECPrivateKey(keypair.PrivateKey)
	if err != nil {
		return nil, err
	}

	x509pub, err := x509.MarshalPKIXPublicKey(keypair.PublicKey)
	if err != nil {
		return nil, err
	}

	skeypair := &SerializedKeypair{}
	skeypair.Uuid = keypair.Uuid
	skeypair.PrivateKey = base64.StdEncoding.EncodeToString(x509priv)
	skeypair.PublicKey = base64.StdEncoding.EncodeToString(x509pub)
	skeypair.MasterKey = base64.StdEncoding.EncodeToString(keypair.MasterKey)

	return skeypair, nil
}

func (keypair *Keypair) Deserialize(data []byte) (*Keypair, error) {
	skeypair := &SerializedKeypair{}
	err := json.Unmarshal(data, &skeypair)
	if err != nil {
		return nil, err
	}

	x509priv, err := base64.StdEncoding.DecodeString(skeypair.PrivateKey)
	if err != nil {
		return nil, err
	}
	x509pub, err := base64.StdEncoding.DecodeString(skeypair.PublicKey)
	if err != nil {
		return nil, err
	}
	masterKey, err := base64.StdEncoding.DecodeString(skeypair.MasterKey)
	if err != nil {
		return nil, err
	}

	privateKey, err := x509.ParseECPrivateKey(x509priv)
	if err != nil {
		return nil, err
	}

	genericPublicKey, _ := x509.ParsePKIXPublicKey(x509pub)
	publicKey := genericPublicKey.(*ecdsa.PublicKey)

	nkeypair := &Keypair{}
	nkeypair.Uuid = skeypair.Uuid
	nkeypair.PrivateKey = privateKey
	nkeypair.PublicKey = publicKey
	nkeypair.MasterKey = masterKey

	return nkeypair, nil
}

func (keypair *Keypair) Encrypt(passphrase []byte) ([]byte, error) {
	serialized, err := keypair.Serialize()
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(serialized)
	if err != nil {
		return nil, err
	}
	data = compression.Deflate(data)

	salt := make([]byte, 16)
	rand.Read(salt)
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)

	block, _ := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)
	return append(salt[:], aesGCM.Seal(nonce, nonce, data, nil)[:]...), nil
}

func (keypair *Keypair) Decrypt(passphrase []byte, data []byte) ([]byte, error) {
	salt, ciphertext := data[:16], data[16:]
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)

	block, err := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]

	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return compression.Inflate(cleartext)
}

func Encrypt(key []byte, buf []byte) ([]byte, error) {
	subkey := make([]byte, 32)
	rand.Read(subkey)

	ecb, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	encsubkey := make([]byte, ecb.BlockSize()*2)
	ecb.Encrypt(encsubkey[:ecb.BlockSize()], subkey[:ecb.BlockSize()])
	ecb.Encrypt(encsubkey[ecb.BlockSize():], subkey[ecb.BlockSize():])

	block, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)

	return append(encsubkey[:], aesGCM.Seal(nonce, nonce, buf, nil)[:]...), nil
}

func Decrypt(key []byte, buf []byte) ([]byte, error) {
	ecb, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	subkey := make([]byte, ecb.BlockSize()*2)

	encsubkey, ciphertext := buf[:ecb.BlockSize()*2], buf[ecb.BlockSize()*2:]
	ecb.Decrypt(subkey[ecb.BlockSize():], encsubkey[ecb.BlockSize():])
	ecb.Decrypt(subkey[:ecb.BlockSize()], encsubkey[:ecb.BlockSize()])

	block, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]
	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return cleartext, nil
}