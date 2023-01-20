// Copyright (C) 2022 by Posit Software, PBC
package rsf

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Define some types that were used in proof-of-concept work. We are keeping
// them here to verify that we haven't broken anything in the new `WriteObject`
// method.

// TODO: We should eventually remove this file, along with `legacy_test.go`.

type checkpointPackageSnippet struct {
	// Summary is required for the UI package listing/searching
	// function, so we store the latest summary with each package.
	Summary string `json:"s" rsf:"s"`

	// The following fields are recorded in the `packages_v2_macro_association`
	// table.
	Version      string `json:"v" rsf:"version"`
	Sha256Sum    string `json:"sha" rsf:"sha,fixed:64"`
	RDep         string `json:"r_dep,omitempty" rsf:"r_dep"`
	Path         string `json:"path,omitempty" rsf:"path"`
	FieldSetHash string `json:"fsh" rsf:"fsh,fixed:32""`

	// FieldSetData is stored in `packages_v2_fieldsets` and provides fast
	// access to the `ShortDescription` data for a package.
	FieldSetData string `json:"fsd" rsf:"fsd"`
}

type fullManifestSnapshot struct {
	Name     string                     `json:"-" rsf:"-"`
	Snapshot string                     `json:"s" rsf:"snapshot,skip,fixed:8"`
	JsonPath string                     `json:"j,omitempty" rsf:"json_path,fixed:64"`
	Deleted  bool                       `json:"d,omitempty" rsf:"deleted"`
	Files    []checkpointPackageSnippet `json:"f,omitempty" rsf:"files"`
}

type fullManifestSnapshotPyPI struct {
	Deleted       bool   `json:"d,omitempty" rsf:"deleted"`
	CanonicalName string `json:"-" rsf:"-"`
	ProjectName   string `json:"-" rsf:"-"`
	Snapshot      string `json:"s" rsf:"snapshot,skip,fixed:8"`
	Version       string `json:"v,omitempty" rsf:"version"`
	Summary       string `json:"u,omitempty" rsf:"summary"`
}

type fullPackageRecord struct {
	Name      string                 `rsf:"name"`
	Snapshots []fullManifestSnapshot `rsf:"snapshots,index:snapshot"`
}

type fullPackageRecordPyPI struct {
	CanonicalName string                     `rsf:"cname"`
	ProjectName   string                     `rsf:"pname"`
	Snapshots     []fullManifestSnapshotPyPI `rsf:"snapshots,index:snapshot"`
}

// writePackage was the original method for writing the information for an individual package.
// Since this was used during the proof-of-concept work, I privatized the method and added a
// unit test that proves that the new generic `WriteObject` method creates identical data.
// We can eventually remove this, but it may be helpful to keep for reference temporarily.
func (f *writer) writePackage(name string, snapshots []fullManifestSnapshot) error {
	var err error

	// Define a few buffers.
	var buf = &bytes.Buffer{}
	var snapshotBuf = &bytes.Buffer{}
	var snapshotIndexBuf = &bytes.Buffer{}

	// Name of package
	_, err = f.WriteStringField(0, name, buf)
	if err != nil {
		return err
	}

	// Record number of snapshots
	_, err = f.WriteSizeField(0, len(snapshots), buf)
	if err != nil {
		return err
	}

	// Write all snapshot data to a separate buffer
	for _, snapshot := range snapshots {
		var snapSz int

		// Write JSON Path
		snapSz, err = f.WriteFixedStringField(snapSz, 64, snapshot.JsonPath, snapshotBuf)
		if err != nil {
			return err
		}

		// Write "deleted" boolean
		snapSz, err = f.WriteBoolField(snapSz, snapshot.Deleted, snapshotBuf)
		if err != nil {
			return err
		}

		// Record number of files
		snapSz, err = f.WriteSizeField(snapSz, len(snapshot.Files), snapshotBuf)
		if err != nil {
			return err
		}

		for _, snippet := range snapshot.Files {
			// summary
			snapSz, err = f.WriteStringField(snapSz, snippet.Summary, snapshotBuf)
			if err != nil {
				return err
			}

			// version
			snapSz, err = f.WriteStringField(snapSz, snippet.Version, snapshotBuf)
			if err != nil {
				return err
			}

			// sha
			snapSz, err = f.WriteFixedStringField(snapSz, 64, snippet.Sha256Sum, snapshotBuf)
			if err != nil {
				return err
			}

			// rdep
			snapSz, err = f.WriteStringField(snapSz, snippet.RDep, snapshotBuf)
			if err != nil {
				return err
			}

			// path
			snapSz, err = f.WriteStringField(snapSz, snippet.Path, snapshotBuf)
			if err != nil {
				return err
			}

			// fieldset hash
			snapSz, err = f.WriteFixedStringField(snapSz, 32, snippet.FieldSetHash, snapshotBuf)
			if err != nil {
				return err
			}

			// fieldset data
			snapSz, err = f.WriteStringField(snapSz, snippet.FieldSetData, snapshotBuf)
			if err != nil {
				return err
			}
		}

		// Record snapshot name and size in separate buffer
		_, err = f.WriteFixedStringField(0, 8, snapshot.Snapshot, snapshotIndexBuf)
		if err != nil {
			return err
		}
		_, err = f.WriteSizeField(0, snapSz, snapshotIndexBuf)
		if err != nil {
			return err
		}
	}

	// Write size of full record
	bs := make([]byte, 4)
	recordSize := buf.Len() + snapshotBuf.Len() + snapshotIndexBuf.Len() + 4
	binary.LittleEndian.PutUint32(bs, uint32(recordSize))
	_, err = f.f.Write(bs)
	if err != nil {
		return err
	}

	// Write initial buffer. This includes the name and the number
	// of snapshots.
	_, err = io.Copy(f.f, buf)
	if err != nil {
		return err
	}

	// Write snapshot index buffer
	_, err = io.Copy(f.f, snapshotIndexBuf)
	if err != nil {
		return err
	}

	// Write snapshots buffer
	_, err = io.Copy(f.f, snapshotBuf)
	if err != nil {
		return err
	}

	return nil
}

// writePackagePyPI was the original method for writing the information for an individual package.
// Since this was used during the proof-of-concept work, I privatized the method and added a
// unit test that proves that the new generic `WriteObject` method creates identical data.
// We can eventually remove this, but it may be helpful to keep for reference temporarily.
func (f *writer) writePackagePyPI(cName, projectName string, snapshots []fullManifestSnapshotPyPI) error {
	var err error

	// Define a few buffers.
	var buf = &bytes.Buffer{}
	var snapshotBuf = &bytes.Buffer{}
	var snapshotIndexBuf = &bytes.Buffer{}

	// Canonical name
	_, err = f.WriteStringField(0, cName, buf)
	if err != nil {
		return err
	}

	// Project name
	_, err = f.WriteStringField(0, projectName, buf)
	if err != nil {
		return err
	}

	// Record number of snapshots
	_, err = f.WriteSizeField(0, len(snapshots), buf)
	if err != nil {
		return err
	}

	// Write all snapshot data to a separate buffer
	for _, snapshot := range snapshots {
		var snapSz int

		// Write "deleted" boolean
		snapSz, err = f.WriteBoolField(snapSz, snapshot.Deleted, snapshotBuf)
		if err != nil {
			return err
		}

		snapSz, err = f.WriteStringField(snapSz, snapshot.Version, snapshotBuf)
		if err != nil {
			return err
		}

		snapSz, err = f.WriteStringField(snapSz, snapshot.Summary, snapshotBuf)
		if err != nil {
			return err
		}

		// Record snapshot name and size in separate buffer
		_, err = f.WriteFixedStringField(0, 8, snapshot.Snapshot, snapshotIndexBuf)
		if err != nil {
			return err
		}
		_, err = f.WriteSizeField(0, snapSz, snapshotIndexBuf)
		if err != nil {
			return err
		}
	}

	// Write size of full record
	bs := make([]byte, 4)
	recordSize := buf.Len() + snapshotBuf.Len() + snapshotIndexBuf.Len() + 4
	binary.LittleEndian.PutUint32(bs, uint32(recordSize))
	_, err = f.f.Write(bs)
	if err != nil {
		return err
	}

	// Write initial buffer. This includes the name and the number
	// of snapshots.
	_, err = io.Copy(f.f, buf)
	if err != nil {
		return err
	}

	// Write snapshot index buffer
	_, err = io.Copy(f.f, snapshotIndexBuf)
	if err != nil {
		return err
	}

	// Write snapshots buffer
	_, err = io.Copy(f.f, snapshotBuf)
	if err != nil {
		return err
	}

	return nil
}
