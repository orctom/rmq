package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func TouchFile(name string) error {
	os.MkdirAll(filepath.Dir(name), 0744)
	file, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0744)
	if err != nil {
		return err
	}
	return file.Close()
}

func ExpandHome(path string) string {
	startsWithTide := strings.HasPrefix(path, "~/")
	startsWithHome := strings.HasPrefix(path, "$HOME")
	if startsWithTide || startsWithHome {
		homeDir, err := os.UserHomeDir()
		if err == nil {
			if startsWithTide {
				return strings.Replace(path, "~", homeDir, 1)
			} else if startsWithHome {
				return strings.Replace(path, "$HOME", homeDir, 1)
			}
		}
	}
	return path
}

func CreateSymlink(source string, target string) error {
	source = ExpandHome(source)
	target = ExpandHome(target)
	if IsNotExists(source) {
		return errors.New("source file not exist")
	}
	if !IsNotExists(target) {
		fmt.Println("del " + target)
		err := os.Remove(target)
		if err != nil {
			return err
		}
	}

	sourceDir := filepath.Dir(source)
	targetDir := filepath.Dir(target)
	if sourceDir == targetDir {
		cwd, err := os.Getwd()
		if err == nil {
			os.Chdir(sourceDir)
			os.Symlink(filepath.Base(source), filepath.Base(target))
			os.Chdir(cwd)
		}
	} else {
		os.Symlink(source, target)
	}
	return nil
}

func IsNotExists(path string) bool {
	stat, err := os.Stat(path)
	return stat == nil || (err != nil && os.IsNotExist(err))
}
