package mqtt

import (
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	ErrInvalidTopic = errors.New("Topic invalid for publishing")
	ErrInvalidSub   = errors.New("Topic invalid for subscribing")
)

// Check if a topic used for subscribing is valid.
func subValid(sub string) bool {
	if len(sub) == 0 || len(sub) > 65535 {
		return false
	}

	if !utf8.Valid([]byte(sub)) {
		return false
	}
	if strings.Contains(sub, "\u0000") {
		return false
	}
	sections := strings.Split(sub, "/")

	for i, s := range sections {
		if strings.Contains(s, "+") {
			if len(s) > 1 {
				return false
			}
		}

		if strings.Contains(s, "#") {
			if len(s) > 1 {
				return false
			}
			if i < len(sections)-1 {
				return false // # can only be the last character
			}
		}
	}

	return true
}

// Check if a topic used for publishing is valid.
func topicValid(topic string) bool {
	if len(topic) == 0 || len(topic) > 65535 {
		return false
	}

	if !utf8.Valid([]byte(topic)) {
		return false
	}

	if strings.Contains(topic, "\u0000") {
		return false
	}
	if strings.Contains(topic, "#") ||
		strings.Contains(topic, "+") {
		return false
	}
	return true
}
func topicMatch(sub, topic string) bool {
	s := strings.Split(sub, "/")
	t := strings.Split(topic, "/")

	// Check for leading '$' first, becuase we don't want to accidentally
	// match it using a wildcard later. '$' topics are reserved for the
	// broker to use and is not for clients.
	if (sub[0] == '$' && topic[0] != '$') ||
		(topic[0] == '$' && sub[0] != '$') {
		return false
	}

	// Work through the subscription section by section and see if it matches the
	// topic exatly or via wildcards.
	for i := 0; i < len(s) && i < len(t); i++ {
		switch s[i] {
		case "+":
			//Single wildcard, dont need to compare
			continue
		case "#":
			if i < len(s)-1 {
				// Invalid, # is not the last character
				return false
			}
			// Everything after this doesn't matter
			return true
		default:
			if s[i] != t[i] {
				// This section of the topic and subscription
				// dont match, so the topic is not a match for
				// the subscription
				return false

			}
		}
	}

	// If we made it this far, everything has matched so far
	if len(s) == len(t) {
		// Topic and subscription both have the same amount of sections
		// and they all matched so return true.
		return true
	} else {
		// the topic and subscription have a mismatch in the number of
		// sections, but if the last character is this wildcard, then
		// everything will match
		if s[len(s)-1] == "#" {
			return true
		}

		// Either the topic or subscription is longer than the other
		// and the wildcard is not on the end, so they are not a match
		return false
	}

}
