# pgCustard collector

A collector you can run close to your database, for gathering statistics and metadata to send to [pgCustard](https://pgcustard.com).

## Requirements

- The database added in pgCustard, and a collector token for it.
- A monitoring user with `pg_monitor`, and optionally `pg_stat_statements` for top queries. See our [setup guides](https://pgcustard.com/docs#setup-guides).
- Outbound HTTPS to app.pgcustard.com, directly or through a proxy.
- 1GB of memory (usual use is 150–250MB).

## Run it

Put the settings in a file, for example `collector.env`:

```
PGHOST=db.internal
PGDATABASE=app
PGUSER=pgcustard
PGPASSWORD=your_password
PGSSLMODE=verify-full
PGSSLROOTCERT=/etc/pgcustard/root.crt
PGCUSTARD_TOKEN=your_token
```

Then start the collector:

```
docker run -d --name pgcustard-collector --restart unless-stopped \
  --env-file collector.env -v /etc/pgcustard:/etc/pgcustard:ro \
  -v pgcustard-collector:/data quay.io/pgcustard/collector:0.1
```

`docker logs pgcustard-collector` should show something like:

```
INFO pgCustard collector 0.1.3 started, collecting from db.internal:5432/app every 30s and sending to https://app.pgcustard.com
INFO collected in 812ms, 1 sent, 0 waiting
```

The collector logs a line per collection. Problems are logged once, and again when they clear. We use `WARN` when the collector can continue, and `ERROR` when it can’t.

Payloads wait in `/data` if pgCustard can’t be reached or isn’t accepting them (before you subscribe, for example), and are sent oldest first once that’s resolved. Payloads over 7 days old are deleted, and so are the oldest ones when they take up more than 1GB in total.

## Settings

| Setting | Notes |
|---|---|
| `PGHOST` | Required, a hostname or IP address (not a Unix socket) |
| `PGPORT` | Defaults to 5432 |
| `PGDATABASE` | Required |
| `PGUSER` | Required |
| `PGPASSWORD` | Or `PGPASSFILE`, the path to a [password file](https://www.postgresql.org/docs/current/libpq-pgpass.html) |
| `PGSSLMODE` | Required: `verify-full` (our recommendation), `verify-ca`, `require`, or `disable` |
| `PGSSLROOTCERT` | The certificate authority to trust: a file, or `amazon-rds` or `supabase` for their bundled certificates (without it, `verify-full` trusts the usual public authorities) |
| `PGCUSTARD_DATABASE_URL` | A `postgres://` URL in place of the settings above. Any it leaves out, like `sslmode`, can still be set on their own. |
| `PGCUSTARD_TOKEN` | Or `PGCUSTARD_TOKEN_FILE`, the path to a file containing it |
| `PGCUSTARD_LOG_LEVEL` | `info` (the default), `warn`, or `error` |
| `HTTPS_PROXY` | A proxy to send through, as `host:port` or `http://host:port` |

## What it sends

- Database statistics and the Postgres version
- Table and index statistics and definitions
- Statistics and texts for top queries, from `pg_stat_statements`, with quoted values and, as a failsafe, any long number that passes the Luhn check (even in comments) replaced by placeholders like `$1`

What might still be sensitive:

- Object names and definitions, including partial-index conditions
- Comments and some (unquoted) numbers in query texts
- The application, user, or prepared transaction ID holding back cleanup
- The name of whichever database is closest to transaction ID wraparound
- Errors from connecting and collecting, which can name hosts and users

`print` collects once and prints the payload instead of sending it, and needs no token:

```
docker run --rm --env-file collector.env -v /etc/pgcustard:/etc/pgcustard:ro \
  quay.io/pgcustard/collector:0.1 print > payload.json
```

It exits with status 1 if it couldn’t collect everything, so it also works as a quick connection check.

To see what it would send over time, `dry-run` collects every 30s like `run`, but keeps the payloads in `/data/dry-run/payloads` instead of sending them.

## Running the collector securely

These steps limit what the collector can reach, and let you check what it sends. They’re designed to suit any database, including ones in scope for things like PCI DSS and HIPAA.

- **Network:** The collector needs a connection to your database and outbound HTTPS to app.pgcustard.com, and nothing inbound. Our replies only say whether a payload was stored, or why not.
- **Encryption:** Set `PGSSLMODE` to `verify-full`.
- **Database role:** Give the collector a role of its own, with `pg_monitor` and nothing else, so it can read statistics but not the data in your tables. Allow it only from the collector’s address in `pg_hba.conf`, for example `hostssl app pgcustard 10.0.5.20/32 scram-sha-256`. The collector keeps one connection open and only reads, so these are safe:

  ```
  alter role pgcustard connection limit 2;
  alter role pgcustard set default_transaction_read_only = on;
  ```

- **Query text:** Consider setting `pg_stat_statements.track_utility` to `off`. Utility statements, like `COPY` or `ALTER`, can include unquoted values that `pg_stat_statements` leaves in.
- **The image:** Pin it by digest (`quay.io/pgcustard/collector@sha256:…`), and treat upgrades as reviewed changes. You can [verify it was built from this code](#verifying-a-release), or [build it yourself](#development). It runs as a non-root user, and has no shell.
- **Secrets:** Pass the password and token as files (`PGPASSFILE`, `PGCUSTARD_TOKEN_FILE`) rather than as environment variables. A token can only send metrics for one database, and you can revoke it from that database’s Collector tokens.
- **Before you switch it on:** Run `dry-run` for a day or two, and review the payloads before switching to `run`.

## Verifying a release

To check a release’s code is ours, verify its signature with the release key on our [security page](https://pgcustard.com/security):

```
version=0.1.3
git clone --branch "v$version" https://github.com/pgmustard/pgcustard-collector.git
curl -fsS https://pgcustard.com/release-key > release-key
git -C pgcustard-collector -c gpg.ssh.allowedSignersFile="$PWD/release-key" verify-tag "v$version"
```

To check the published image was built from that code, rebuild it and compare the digests:

```
pgcustard-collector/build-image.sh
docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "quay.io/pgcustard/collector:$version"
```

## Development

The collector is written in Clojure, and built and tested with [Leiningen](https://leiningen.org):

```
lein test
```

Tests that need a Postgres are skipped without one:

```
PGCUSTARD_TEST_POSTGRES_URL=postgresql://postgres@localhost/postgres lein test
```

To build the image:

```
docker build -t pgcustard-collector .
```

## Issues and pull requests

Issues are welcome. We develop the collector alongside pgCustard and copy it here for each release, so we can’t merge pull requests as they are, but we can apply a change you suggest.

## Security

To report a security issue, please email security@pgcustard.com with steps to reproduce it, rather than opening an issue.

## Licence

Apache 2.0, see [LICENSE](LICENSE). The libraries the collector includes, and their licences, are listed in [THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES).
