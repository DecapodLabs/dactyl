# Dactyl Supabase RPC contract v1

This is the shared, generic wire contract for the coordinated integration.
Production SQL, grants, entitlement and task/event policy stay in private
Propodus. Dactyl owns transport and normalization; Decapod owns its workflow and
opaque context. The existing Supabase service `/query` and `/batch` routes retain
their meaning. Native RPC is explicitly selected, never inferred from a host.

## Configuration and authentication

An explicit RPC route supplies four independent values: Supabase project origin,
publishable project API key, current user access-token bearer, and API schema.
Contract v1 uses the API schema `dactyl_api` by default; a valid explicit schema
identifier may override it. The project origin has no credentials, query,
fragment or non-root path. HTTPS with certificate validation is mandatory except
literal loopback HTTP for disposable tests. The API key is a publishable key,
never a secret/service-role key or database password. Keys and bearers are
redacted from Debug, errors, receipts and serialized storage context.

Every request is a POST with:

```text
Authorization: Bearer <current user access token>
apikey: <publishable project API key>
Content-Type: application/json
Accept: application/json
Content-Profile: <api schema>
Accept-Profile: <api schema>
Dactyl-Contract-Version: 1
```

The session owner supplies and persists the bearer. Dactyl provides stateless
managed-auth transport through `CloudConnector`; the caller owns browser
interaction and durable session custody. Dactyl never stores refresh tokens,
creates users, or retries a request after authentication failure. Supabase must
independently trust the issuer/signature and restrict
the JWT role; a project API key is not resource authorization. The private RPC
checks authenticated identity, active session, membership, grants and entitlement
before accessing any requested resource. Context is an assertion, not permission.

## Requests

`POST <project origin>/rest/v1/rpc/dactyl_query_v1` accepts one named PostgreSQL
JSONB parameter, `request`:

```json
{"request":{"version":1,"kind":"read","sql":"SELECT $1 AS value","params":[42],"access_mode":"read_only","context":{"version":1,"payload":{"application_owned":"opaque"}}}}
```

The SQL is a generic fixture example, not a production SQL grant. `kind` is
`read` or `write`. A `read` call always sends `read_only`. The private server
recognizes only its published production profile and executes server-owned
statements with bound values. It never executes arbitrary supplied SQL.

`POST <project origin>/rest/v1/rpc/dactyl_batch_v1` accepts:

```json
{"request":{"version":1,"operations":[{"kind":"write","sql":"fixture statement","params":[]},{"kind":"read","sql":"fixture observation","params":[]}],"access_mode":"read_write","context":{"version":1,"payload":{"application_owned":"opaque"}}}}
```

Operations retain their order and existing `kind`, `sql` and `params` fields.
`schema` remains representable but the production task profile rejects it as a
capability error. Dactyl forwards the entire versioned context unchanged and
does not inspect application fields. Empty atomic lists return no results and
make no HTTP request. An unsupported request version is a protocol error.

## Results and errors

A successful query returns the existing object shape:

```json
{"columns":["value"],"rows":[{"value":42}],"affected_rows":0,"generated_keys":[]}
```

A successful batch returns `{"results":[...]}`, exactly one result per operation
in original order. Counts are explicit unsigned integers; generated keys are
explicit signed integers or text, never inferred. Columns are ordered unique
names and each object row has exactly those keys. Missing fields, malformed
JSON and result-count mismatch are protocol errors. Zero-row CAS is a successful
physical result; the application owns conflict interpretation.

Parameters and cells support NULL, bool, signed 64-bit integer, finite real,
text and blobs as JSON byte arrays. Objects, unsupported arrays, oversized
integers and non-finite floats fail explicitly. No base64 guessing or conversion
of text into blobs is allowed.

Errors use a non-2xx HTTP response and PostgREST's top-level
`{"code":"stable_code","message":"safe text","details":null,"hint":null}`
envelope. Private functions can raise PostgREST custom errors with fixed public
codes. Dactyl maps its existing allowlisted service codes, plus recognized
PostgreSQL/PostgREST codes, to typed authentication, authorization, capability,
constraint/conflict, read-only, timeout, unavailable and protocol errors.
`PGRST106` (API schema not exposed) maps to `Capability/unsupported_schema`. Unknown
codes/messages/details/hints are redacted; HTTP status provides a bounded
fallback. PostgreSQL 23505/23503/23502/23514 are constraints, 40001/40P01 are
conflicts, 57014 is timeout, 25006 is read-only, and 42501 is authorization.
PGRST202 denotes an unavailable RPC capability; PGRST301/302/303 authentication.
Redirect responses fail without forwarding either credential.

## Transaction and bounded execution

One `atomic` call produces one batch RPC. One RPC executes in one PostgreSQL
transaction; server-side operation, authorization, profile or result-construction
errors before returning abort the whole batch. A client response-decoding or
normalization failure, like a lost response, does not establish rollback: the
transaction may already have committed. Never retry an unknown write outcome. A read operation may never mutate even inside a writable
batch. A read-only request rejects all mutation. Private task-state/event writes
remain inseparable; unsuccessful/stale transitions leave no successful event.

Dactyl keeps its 5-second connect, 15-second full-request and 4 MiB JSON bounds.
Private limits can be stricter and return explicit quota/capability failures.
There are no automatic write/batch retries: timeout or lost response can follow
a commit. Only independently observed state can resolve that ambiguity.

## Compatibility and evidence

Use an additive native-RPC constructor/configuration. Existing route constructors,
OpenOptions and public Connection operations remain source compatible. Native
RPC does not alter ambient datastore defaults or SQLite/Neon semantics.

Generic synthetic transport, real PostgreSQL, private task/entitlement, real CLI
init/session reuse, and hosted Supabase are distinct proof profiles. Generic
fixture SQL stays disposable and never expands the production allowlist. Release
handoff requires a verified published registry artifact and a clean consumer;
Git/path patches are development evidence only.

Normative external mechanics:
- [PostgREST function arguments](https://docs.postgrest.org/en/v12/references/api/functions.html)
- [PostgREST errors](https://docs.postgrest.org/en/v12/references/errors.html)
- [Supabase JWT trust](https://supabase.com/docs/guides/auth/jwts)

