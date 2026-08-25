package server

import "os"

func processID() int { return os.Getpid() }

func executablePath() string {
	p, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	return p
}
