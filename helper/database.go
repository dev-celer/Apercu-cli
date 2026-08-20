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

type FullRelationName struct {
	Schema string `json:"schema" yaml:"schema"`
	Table  string `json:"table" yaml:"table"`
}

type QueryWithAffectedTables struct {
	Query          string             `json:"query" yaml:"query"`
	AffectedTables []FullRelationName `json:"affected_tables" yaml:"affected_tables"`
}

func (t FullRelationName) String() string {
	if strings.Contains(t.Schema, ".") {
		t.Schema = fmt.Sprintf("\"%s\"", t.Schema)
	}
	if strings.Contains(t.Table, ".") {
		t.Table = fmt.Sprintf("\"%s\"", t.Table)
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

func (t FullRelationName) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *FullRelationName) UnmarshalText(data []byte) error {
	s := string(data)
	if before, after, ok := strings.Cut(s, "."); ok {
		t.Schema, t.Table = before, after
	} else {
		t.Schema, t.Table = "public", s
	}
	return nil
}

func (t FullRelationName) MarshalYAML() (any, error) {
	return t.String(), nil
}

func (t *FullRelationName) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return t.UnmarshalText([]byte(s))
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
