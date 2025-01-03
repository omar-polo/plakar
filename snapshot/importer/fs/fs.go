/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

package fs

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/importer"
)

type FSImporter struct {
	importer.ImporterBackend
	rootDir string
}

func init() {
	importer.Register("fs", NewFSImporter)
}

func NewFSImporter(location string) (importer.ImporterBackend, error) {
	var err error

	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	location, err = filepath.Abs(location)
	if err != nil {
		return nil, err
	}

	return &FSImporter{
		rootDir: location,
	}, nil
}

func (p *FSImporter) Origin() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	return hostname
}

func (p *FSImporter) Type() string {
	return "fs"
}

func (p *FSImporter) Scan() (<-chan importer.ScanResult, error) {
	return walkDir_walker(p.rootDir, 256)
}

func (p *FSImporter) NewReader(pathname string) (io.ReadCloser, error) {
	return os.Open(pathname)
}

func (p *FSImporter) Close() error {
	return nil
}

func (p *FSImporter) Root() string {
	return p.rootDir
}
