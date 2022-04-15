package ioutil2

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	doWriteTest("/tmp/atomic-file-test.txt", t)
	doWriteTest("atomic-file-test.txt", t)

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	doWriteTest(filepath.Join(tmpDir, "atomic-file-test.txt"), t)
}

func doWriteTest(fname string, t *testing.T) {
	err := WriteFileAtomic(fname, []byte("test string\n"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fname); err != nil {
		t.Fatal(err)
	}
}

func TestCreateFileIfNotExists(t *testing.T) {
	t.Run("New file that doesn't exist", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		testFile := filepath.Join(tmpDir, "test-file.txt")
		if err := CreateFileIfNotExists(testFile, 0644); err != nil {
			t.Fatal(err)
		}
		data, err := ioutil.ReadFile(testFile)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 0 {
			t.Fatal(errors.New("new file contents should be empty"))
		}
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("File that already exists", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		existingFile := filepath.Join(tmpDir, "existing-file.txt")
		if err := ioutil.WriteFile(existingFile, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := CreateFileIfNotExists(existingFile, 0644); err != nil {
			t.Fatal(err)
		}
		data, err := ioutil.ReadFile(existingFile)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal([]byte("test"), data) {
			t.Fatal(errors.New("existing file contents should not be modified"))
		}
	})
}

func TestTempFile_ReadWriteable(t *testing.T) {
	file, err := UnlinkedTempFile("", "test_temp_file-*")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, file.Close())
	}()

	testStr := "testing"
	n, err := file.WriteString(testStr)
	require.NoError(t, err)
	require.Equal(t, len(testStr), n)

	offset, err := file.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	actualBytes, err := ioutil.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, testStr, string(actualBytes))
}

func TestTempFile_DeletesOnClose(t *testing.T) {
	file, err := UnlinkedTempFile("", "test_temp_file-*")
	require.NoError(t, err)
	name := file.Name()

	require.NoError(t, file.Close())

	_, err = ioutil.ReadFile(name)
	require.Error(t, err)
}
