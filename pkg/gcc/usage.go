// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import "fmt"

type usage int

const (
	// usageNormal is the zero value: a fresh estimator that has seen no
	// feedback reports "normal", not "overuse" — an SFU gating spatial
	// upswitches on the last overuse signal read a just-joined
	// subscriber's zero value as a queue and held its first upswitch for
	// the whole congestion memory (5 s).
	usageNormal usage = iota
	usageOver
	usageUnder
)

func (u usage) String() string {
	switch u {
	case usageOver:
		return "overuse"
	case usageUnder:
		return "underuse"
	case usageNormal:
		return "normal"
	default:
		return fmt.Sprintf("invalid usage: %d", u)
	}
}
