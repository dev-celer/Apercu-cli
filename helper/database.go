package helper

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type ConnectionFields struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database" yaml:"database"`
	Url      string `json:"url" yaml:"url"`
}

type FullTableName struct {
	Schema string `json:"schema" yaml:"schema"`
	Table  string `json:"table" yaml:"table"`
}

func (t FullTableName) String() string {
	if strings.Contains(t.Schema, ".") {
		t.Schema = fmt.Sprintf("\"%s\"", t.Schema)
	}
	if strings.Contains(t.Table, ".") {
		t.Table = fmt.Sprintf("\"%s\"", t.Table)
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

var reg = regexp.MustCompile(`postgresql:\/\/(.+?):(.+?)@(.+?)[\/:](\d*)\/?(.+?)?(?:\?|$)`)

func ExtractConnectionFieldsFromUrl(databaseUrl string) (ConnectionFields, error) {
	matches := reg.FindStringSubmatch(databaseUrl)

	portStr := matches[4]
	if portStr == "" {
		portStr = "5432"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to parse port from database url: %v", err))
	}

	return ConnectionFields{
		Host:     matches[3],
		Port:     port,
		User:     matches[1],
		Password: matches[2],
		Database: matches[5],
		Url:      databaseUrl,
	}, nil
}
