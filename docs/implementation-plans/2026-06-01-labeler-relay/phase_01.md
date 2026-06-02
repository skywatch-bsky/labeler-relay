# Labeler Relay Implementation Plan — Phase 1

**Goal:** A buildable Go module with the output lexicon defined and Go types (with CBOR marshalling) generated.

**Architecture:** Greenfield Go service that reuses Bluesky's `indigo` library for event fan-out. This phase scaffolds the module, defines the custom output lexicon `community.labeler.sync.subscribeLabelers`, and generates Go structs + CBOR codec for it by mirroring indigo's lexgen/cbor-gen toolchain.

**Tech Stack:** Go 1.22+, `github.com/bluesky-social/indigo`, indigo `cmd/lexgen` + `whyrusleeping/cbor-gen`.

**Scope:** 7 phases from original design. This is Phase 1 of 7.

**Codebase verified:** 2026-06-01. Repo is greenfield (only `docs/` and `.git/` present). indigo inspected at commit `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`.

**Design decisions carried into this plan (confirmed with operator):**
- **Custom output types.** We define our own `community.labeler.sync.subscribeLabelers` Go types whose `#labels`/`#service`/`#info` payloads carry the relay-minted `seq`. We do NOT reuse `comatproto.LabelSubscribeLabels_*` for output (those carry the upstream seq and would muddy provenance).
- Upstream `Sig` bytes are passed through verbatim. The relay never re-signs.

---

## Acceptance Criteria Coverage

**Verifies: None.** This is an infrastructure/scaffolding phase. Verification is operational (`go build ./...` succeeds, lexicon validates, generated types compile). No acceptance criteria are exercised here.

---

## Key research findings (ground truth from indigo @ 5368f553)

- Module path: `github.com/bluesky-social/indigo`.
- Lexicon codegen is a two-step pipeline:
  1. `cmd/lexgen` reads lexicon JSON from a `LEXDIR` and generates Go structs (`lex.ReadSchema` → `lex.Run`).
  2. `gen/main.go` runs `whyrusleeping/cbor-gen` (`cbg.Gen.WriteMapEncodersToFile`) to generate `*_cbor.go` CBOR marshalling for those structs.
- indigo's own `Makefile` targets: `lexgen` (JSON→Go) and `cborgen` (Go→`*_cbor.go`).
- `lexutil.LexBytes` (alias for `[]byte`) is the correct type for raw signature/CBOR bytes and round-trips losslessly.

**NOTE ON CODEGEN COUPLING:** indigo's `cmd/lexgen` build-file (`cmd/lexgen/bsky.json`) maps lexicon prefixes to output Go packages and is geared to indigo's own `api/atproto` + `api/bsky` layout. For a single external lexicon we do NOT need the full lexgen pipeline — it is heavyweight and tightly coupled to indigo's repo layout. Instead this phase hand-writes the small Go structs for our output types and uses `cbor-gen` (a standalone, importable library) directly via a local `gen/` program. This is the same `cbor-gen` indigo uses, just invoked on our own structs. This avoids vendoring indigo's lexgen build config while still producing identical CBOR wire output.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Initialize Go module and pin indigo

**Files:**
- Create: `go.mod`
- Create: `.gitignore`

**Step 1: Initialize the module**

Run:
```bash
go mod init github.com/scarndp/labeler-relay
```
(Adjust the module path to the intended repo URL if different; use this exact path for the rest of the plan.)

**Step 2: Add the indigo dependency**

Run:
```bash
go get github.com/bluesky-social/indigo@5368f55344e0b5e203ab4d9501e1ddebd9983ad7
```

This will resolve indigo and its transitive deps (including `github.com/whyrusleeping/cbor-gen`, `github.com/RussellLuo/slidingwindow`, `github.com/gorilla/websocket`). Do not hand-edit `go.mod` versions; let `go get` resolve.

**If the bare SHA does not resolve** (some proxies require a pseudo-version), pin instead to the nearest pseudo-version that contains the verified API surface (the `EventPersistence` interface, `RepoStreamCallbacks` label callbacks, `EventManager.Subscribe`, `atproto/labeling.Label.VerifySignature`). Run `go list -m -versions github.com/bluesky-social/indigo` to find a candidate, pick one at or after the verified commit, build, and confirm the referenced symbols exist. **Surface the chosen version to the operator** before proceeding — later phases reference indigo internals verified at `5368f553`, and a wildly newer version could have moved them.

**Step 3: Create `.gitignore`**

```gitignore
/labeler-relay
/*.db
/*.db-wal
/*.db-shm
/.worktrees/
*.test
/tmp/
```

**Step 4: Verify operationally**

Run: `go mod tidy`
Expected: completes without error; `go.mod` and `go.sum` are populated with indigo.

Run: `go build ./...`
Expected: no packages yet to build (or trivially succeeds). No errors.

**Step 5: Commit**

```bash
git add go.mod go.sum .gitignore
git commit -m "chore: initialize go module and pin indigo dependency"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Define the output lexicon JSON

**Files:**
- Create: `lexicons/community/labeler/sync/subscribeLabelers.json`

**Step 1: Write the lexicon**

This defines the unified output subscription. It is a union of `#labels`, `#service`, and `#info`, takes a `cursor` query param, and declares a `FutureCursor` error. Each message carries the relay-minted `seq`.

```json
{
  "lexicon": 1,
  "id": "community.labeler.sync.subscribeLabelers",
  "defs": {
    "main": {
      "type": "subscription",
      "description": "Subscribe to an aggregated, re-sequenced stream of labels and labeler service records from multiple upstream labelers. Each message carries a single monotonic relay-assigned seq.",
      "parameters": {
        "type": "params",
        "properties": {
          "cursor": {
            "type": "integer",
            "description": "The last known relay seq. Backfill resumes from seq > cursor."
          }
        }
      },
      "message": {
        "schema": {
          "type": "union",
          "refs": [
            "#labels",
            "#service",
            "#info"
          ]
        }
      },
      "errors": [
        {
          "name": "FutureCursor",
          "description": "The provided cursor is greater than the current head seq."
        }
      ]
    },
    "labels": {
      "type": "object",
      "description": "A batch of labels relayed from an upstream labeler, carrying the relay-minted seq. Label bytes (including sig) are byte-faithful to the upstream frame.",
      "required": ["seq", "src", "labels"],
      "properties": {
        "seq": {
          "type": "integer",
          "description": "Relay-minted global sequence number."
        },
        "src": {
          "type": "string",
          "format": "did",
          "description": "DID of the origin labeler these labels came from."
        },
        "labels": {
          "type": "array",
          "items": {
            "type": "ref",
            "ref": "com.atproto.label.defs#label"
          }
        }
      }
    },
    "service": {
      "type": "object",
      "description": "A labeler service record (full app.bsky.labeler.service) observed via discovery, carrying the relay-minted seq.",
      "required": ["seq", "src", "record"],
      "properties": {
        "seq": {
          "type": "integer",
          "description": "Relay-minted global sequence number."
        },
        "src": {
          "type": "string",
          "format": "did",
          "description": "DID of the labeler whose service record this is."
        },
        "record": {
          "type": "ref",
          "ref": "app.bsky.labeler.service"
        }
      }
    },
    "info": {
      "type": "object",
      "description": "An informational frame, e.g. OutdatedCursor when a cursor falls below the retention floor.",
      "required": ["name"],
      "properties": {
        "name": {
          "type": "string",
          "knownValues": ["OutdatedCursor"]
        },
        "message": {
          "type": "string"
        }
      }
    }
  }
}
```

**Step 2: Verify it is valid JSON**

Run:
```bash
python3 -m json.tool lexicons/community/labeler/sync/subscribeLabelers.json > /dev/null && echo OK
```
Expected: prints `OK`.

**Step 3: Commit**

```bash
git add lexicons/community/labeler/sync/subscribeLabelers.json
git commit -m "feat: define community.labeler.sync.subscribeLabelers output lexicon"
```
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
<!-- START_TASK_3 -->
### Task 3: Hand-write output Go types and generate CBOR marshalling

**Files:**
- Create: `api/community/types.go` (the Go structs for our output lexicon)
- Create: `gen/main.go` (cbor-gen driver)

**Step 1: Write the output type structs**

These mirror the lexicon defs from Task 2. They embed indigo's generated `comatproto.LabelDefs_Label` and `bsky.LabelerService` by reference so label/record bytes stay identical to upstream. Field tags use `cborgen:` exactly as indigo's generated types do, so cbor-gen produces matching wire output.

Create `api/community/types.go`:

```go
package community

import (
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
)

// LabelerSyncSubscribeLabelers_Labels is the #labels variant of the
// community.labeler.sync.subscribeLabelers output stream. Seq is the
// relay-minted global sequence; Labels are byte-faithful to upstream.
type LabelerSyncSubscribeLabelers_Labels struct {
	Seq    int64                         `json:"seq" cborgen:"seq"`
	Src    string                        `json:"src" cborgen:"src"`
	Labels []*comatproto.LabelDefs_Label `json:"labels" cborgen:"labels"`
}

// LabelerSyncSubscribeLabelers_Service is the #service variant, carrying a
// full app.bsky.labeler.service record under the relay-minted Seq.
//
// FIDELITY NOTE: unlike #labels (whose label Sig bytes are passed through
// verbatim), the #service record is decoded from the firehose CAR into a typed
// *bsky.LabelerService and RE-ENCODED here. It is therefore NOT byte-faithful
// to the upstream bytes — it is field-faithful (all fields preserved) but the
// CBOR encoding may differ. No acceptance criterion requires service-record
// signature fidelity; service records are advisory metadata, not signed labels.
// Only labels are byte-faithful in this relay.
type LabelerSyncSubscribeLabelers_Service struct {
	Seq    int64                 `json:"seq" cborgen:"seq"`
	Src    string                `json:"src" cborgen:"src"`
	Record *bsky.LabelerService  `json:"record" cborgen:"record"`
}

// LabelerSyncSubscribeLabelers_Info is the #info variant (e.g. OutdatedCursor).
type LabelerSyncSubscribeLabelers_Info struct {
	Name    string  `json:"name" cborgen:"name"`
	Message *string `json:"message,omitempty" cborgen:"message,omitempty"`
}
```

**Step 2: Write the cbor-gen driver**

cbor-gen is a standalone library. The driver registers our structs and writes a `*_cbor.go` file. This mirrors indigo's `gen/main.go` pattern (`cbg.Gen.WriteMapEncodersToFile`).

Create `gen/main.go`:

```go
package main

import (
	"fmt"
	"os"

	cbg "github.com/whyrusleeping/cbor-gen"

	community "github.com/scarndp/labeler-relay/api/community"
)

func main() {
	if err := cbg.WriteMapEncodersToFile(
		"api/community/cbor_gen.go",
		"community",
		community.LabelerSyncSubscribeLabelers_Labels{},
		community.LabelerSyncSubscribeLabelers_Service{},
		community.LabelerSyncSubscribeLabelers_Info{},
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

**Step 3: Generate the CBOR code**

Run:
```bash
go run ./gen
```
Expected: creates `api/community/cbor_gen.go` with `MarshalCBOR`/`UnmarshalCBOR` for all three types. No errors.

**Step 4: Verify it compiles**

Run: `go build ./...`
Expected: builds without error. `api/community` package compiles with both hand-written types and generated CBOR.

**Step 5: Commit**

```bash
git add api/community/types.go api/community/cbor_gen.go gen/main.go
git commit -m "feat: generate CBOR types for subscribeLabelers output lexicon"
```
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add a Makefile target for regenerating CBOR

**Files:**
- Create: `Makefile`

**Step 1: Write the Makefile**

```makefile
.PHONY: build test cborgen lint

build:
	go build ./...

test:
	go test ./...

cborgen: ## Regenerate CBOR marshalling for output lexicon types
	go run ./gen
	go build ./...

lint:
	go vet ./...
```

**Step 2: Verify operationally**

Run: `make cborgen`
Expected: regenerates `api/community/cbor_gen.go` (no diff if already current) and builds clean.

Run: `make build`
Expected: builds without error.

**Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: add make targets for build, test, and cborgen"
```
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->

<!-- START_TASK_5 -->
### Task 5: Create the entry point stub

**Files:**
- Create: `cmd/labeler-relay/main.go`

**Step 1: Write a minimal main**

This is a wiring stub. It compiles and runs but does nothing yet — later phases fill in the wiring. It must NOT contain TODO-driven dead code; it is a real (if empty) program.

```go
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "labeler-relay:", err)
		os.Exit(1)
	}
}

// run is the real entry point. Wiring for store, slurper, discovery, and the
// output server is added in later phases.
func run() error {
	fmt.Println("labeler-relay: not yet wired")
	return nil
}
```

**Step 2: Verify operationally**

Run: `go build ./...`
Expected: builds without error.

Run: `go run ./cmd/labeler-relay`
Expected: prints `labeler-relay: not yet wired` and exits 0.

**Step 3: Commit**

```bash
git add cmd/labeler-relay/main.go
git commit -m "feat: add labeler-relay entry point stub"
```
<!-- END_TASK_5 -->

---

## Phase 1 Done When

- `go build ./...` succeeds.
- `lexicons/community/labeler/sync/subscribeLabelers.json` is valid JSON.
- `api/community/cbor_gen.go` is generated and the `api/community` package compiles.
- `go run ./cmd/labeler-relay` runs and exits cleanly.

If `go get` on the pinned indigo commit fails to resolve, surface to the operator before proceeding — do not silently float to a different version, as later phases reference indigo internals verified at `5368f553`.
