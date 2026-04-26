package database_url

import (
	"fmt"
	"regexp"
	"strings"
)

var rewriteRegex = regexp.MustCompile(`postgresql:\/\/(?:.+?)@(.+?)(:\d*)?(?:\/|\?|$)`)

func RewriteDatabaseUrlHostAndPort(url string, host string, port string) (string, error) {
	m := rewriteRegex.FindStringSubmatchIndex(url)
	if m == nil {
		return "", fmt.Errorf("not a valid postgresql url: %q", url)
	}

	hostStart, hostEnd := m[2], m[3]
	portStart, portEnd := m[4], m[5]

	var b strings.Builder
	b.WriteString(url[:hostStart])
	b.WriteString(host)
	b.WriteString(":")
	b.WriteString(port)
	if portStart == -1 {
		b.WriteString(url[hostEnd:])
	} else {
		b.WriteString(url[portEnd:])
	}
	return b.String(), nil
}
