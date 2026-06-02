# Admin

Last verified: 2026-06-02

## Purpose
Provides authenticated HTTP endpoints for manual labeler registry management. Allows operators to add, enable, disable, and list labelers independently of firehose auto-discovery.

## Contracts
- **Exposes**: `API` (Routes), `RequireBearer` middleware
- **Guarantees**:
  - All routes require Bearer token auth (constant-time comparison)
  - POST /admin/labelers resolves endpoint from DID before upserting with source="manual"
  - Enable/disable/delete trigger slurper reconcile via poke callback
  - Manual labelers stick: source="manual" is preserved by the store's upsert stickiness rule
- **Expects**: Valid admin token configured. DIDResolver can reach the network.

## Dependencies
- **Uses**: store (LabelerRegistry, Labeler), firehose (DIDResolver interface)
- **Used by**: main.go (mounts at /admin/)
- **Boundary**: Must not import slurper, server, metrics, or config

## Routes
| Method | Path | Action |
|--------|------|--------|
| POST | /admin/labelers | Add labeler (resolve endpoint, upsert, poke) |
| GET | /admin/labelers | List all labelers |
| DELETE | /admin/labelers/{did} | Disable labeler |
| POST | /admin/labelers/{did}/enable | Enable labeler |
| POST | /admin/labelers/{did}/disable | Disable labeler |

## Key Files
- `admin.go` - API struct, Routes, all handlers
- `auth.go` - RequireBearer middleware (constant-time token comparison)
