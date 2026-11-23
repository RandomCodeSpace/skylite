---
title: Configuration reference
linkTitle: Configuration
description: Every key of the server's JSON configuration file, with its type, default and meaning, and the environment variables the server reads.
weight: 60
navgroup: reference
status: current
---

`skylite serve -config <file>` reads one JSON object from `<file>`. The server refuses unknown keys, trailing data after the object and any value that fails validation, and does not start with a configuration it refuses; the error says which key is wrong. The file never holds a secret: credentials come from files, environment variables or the cloud SDKs' default credential chains.

The [CLI reference](/docs/reference/cli/) lists the commands and exit statuses.

## Example

A server with TLS, an OIDC issuer, Azure Blob Storage with Entra ID, two pods that forward to each other, and a readiness probe:

```json
{
  "listen": ":8443",
  "tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},
  "auth": {
    "issuers": [
      {"issuer": "https://issuer.example.com", "audience": "skylite"}
    ],
    "rules": [
      {"issuer": "https://issuer.example.com", "claim": "groups", "value": "skylite-admins", "database": "*", "role": "admin"},
      {"issuer": "https://issuer.example.com", "subject": "orders-service", "database": "orders", "role": "write"}
    ]
  },
  "storage": {
    "prefix": "prod",
    "azure": {"account": "mystorageaccount", "container": "skylite", "entra": true}
  },
  "databases": ["orders"],
  "peer": {
    "listen": ":8444",
    "address_env": "SKYLITE_PEER_ADDRESS",
    "cert_file": "/etc/skylite/peer.crt",
    "key_file": "/etc/skylite/peer.key",
    "ca_file": "/etc/skylite/peer-ca.crt",
    "server_name": "skylite-peer"
  },
  "probe_listen": ":8081"
}
```

## Top level

| Key | Type | Default | Meaning |
|---|---|---|---|
| `listen` | string | required | TCP address of the client listener, such as `:8443` or `127.0.0.1:8080`. |
| `tls` | object | none | The server's certificate and key; see [`tls`](#tls). Exactly one of `tls` and `plaintext` must be set. |
| `plaintext` | boolean | `false` | `true` serves HTTP/1.1 and HTTP/2 (prior-knowledge h2c) without TLS, for a server behind a mesh or proxy that terminates TLS, or for a local trial. |
| `auth` | object | required | Token issuers and access rules; see [`auth`](#auth). |
| `storage` | object | required | The object store that holds the databases; see [`storage`](#storage). |
| `databases` | array of strings | empty | The databases this server leads, or follows, and answers queries for. At most 13 names, each listed once. A name is 1 to 63 characters from `a-z`, `0-9` and `-`, and starts with a letter or digit. A listed database need not exist yet: once it is created, the server takes office within about 20 seconds. With no databases the server only creates databases (`PUT`). |
| `follower_reads` | boolean | `true` when `peer` is set, otherwise `false` | Lets a pod that does not lead a database answer stateless reads for it from a fresh local follower. Set it to `false` to serve reads only on the leader. |
| `peer` | object | none | The pod-to-pod listener; see [`peer`](#peer). Without it, a pod answers `not_serving` for a database it does not serve, instead of forwarding the request. |
| `probe_listen` | string | none | TCP address of a plain HTTP listener that answers `GET /readyz`, without authentication: `200` while the server leads at least one configured database or has a fresh follower for one, `503` otherwise and from the start of shutdown. See [Readiness](/docs/server/#readiness). |

## `tls`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `tls.cert_file` | string | required with `tls` | PEM certificate chain. Read once, at start; a rotated certificate needs a restart. |
| `tls.key_file` | string | required with `tls` | PEM private key. Read once, at start. |

The server accepts TLS 1.2 or later and serves HTTP/1.1 and HTTP/2.

## `auth`

Every request carries an OIDC JWT as a bearer token. The server verifies it against the configured issuers, then grants a role from the rules. Tokens must be signed with RS256 (RSA keys of at least 2,048 bits), ES256 or EdDSA. A principal no rule matches is denied everything.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `auth.issuers` | array | required, at least one | The token issuers the server trusts. Each issuer appears once. |
| `auth.issuers[].issuer` | string | required | The exact value tokens carry in `iss`. Without `jwks_file` it must be an `https` URL with no query, fragment, user information or white space, and the server fetches the issuer's signing keys through OIDC discovery (`<issuer>/.well-known/openid-configuration`). |
| `auth.issuers[].audience` | string | required | A value the token's `aud` claim must contain. |
| `auth.issuers[].jwks_file` | string | none | A JSON Web Key Set file holding the issuer's public keys, used instead of OIDC discovery; no network request is made for this issuer. The server does not start if the file is missing, unreadable or holds no usable key (each key needs a `kid`). It reads the file again periodically, so a rotated file is picked up without a restart. |
| `auth.rules` | array | empty | Access rules. With none, every request is refused with `403`. |
| `auth.rules[].issuer` | string | required | One of the configured issuers. |
| `auth.rules[].subject` | string | | Matches the token's `sub` exactly. Exactly one of `subject` and `claim` is set. |
| `auth.rules[].claim` | string | | A top-level claim name, such as `groups` or `roles`. The rule matches when the claim equals `value`, or, for an array claim, contains it. |
| `auth.rules[].value` | string | required with `claim` | The value the claim must equal or contain. |
| `auth.rules[].database` | string | required | A database name, or a pattern in which `*` matches any run of characters and `?` one character, such as `*` or `orders-*`. |
| `auth.rules[].role` | string | required | `read`, `write` or `admin`. Each role includes the ones before it. |

What each role allows:

| Role | Allows |
|---|---|
| `read` | Queries and `describe`; closing and cancelling one's own channels. |
| `write` | Everything `read` allows, plus opening write channels: writes and transactions. |
| `admin` | Everything `write` allows, plus creating databases (`PUT`). |

## `storage`

`storage` holds `prefix` and exactly one of `localfs`, `s3`, `gcs` and `azure`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `storage.prefix` | string | empty | A key prefix: segments separated by `/`, each made of letters, digits, `.`, `_` and `-`, not `.` or `..`, not ending in `.`, with no empty segment (so no leading or trailing `/`). At most 28 segments and 873 bytes. A database's objects live under `<prefix>/<database>/`, or `<database>/` without a prefix. |

### `storage.localfs`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `storage.localfs.path` | string | required | A local directory. For trials and tests: the durability of the databases is that of this directory. |

### `storage.azure`

See [Azure Blob Storage](/docs/azure-blob/) for setup, permissions and credentials.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `storage.azure.account` | string | required | The storage account name. |
| `storage.azure.container` | string | required | The container. It must exist; the server never creates it. |
| `storage.azure.endpoint` | string | `https://<account>.blob.core.windows.net` | Another blob endpoint, such as Azurite's. With `entra`, it must be `https`. |
| `storage.azure.shared_key_file` | string | | Path of a file holding the account's base64 shared key. |
| `storage.azure.shared_key_env` | string | | Name of an environment variable holding the account's base64 shared key. |
| `storage.azure.entra` | boolean | `false` | `true` authenticates with Entra ID through the Azure SDK's `DefaultAzureCredential`. |

Exactly one of `shared_key_file`, `shared_key_env` and `entra` is set.

### `storage.s3`

Amazon S3 and S3-compatible stores. Tested against the RustFS emulator only, not against Amazon S3. Credentials come from the AWS SDK's default chain: environment variables, shared configuration files, web identity, then the instance or task role.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `storage.s3.bucket` | string | required | The bucket. It must exist. |
| `storage.s3.region` | string | required | The bucket's region, used to sign requests. |
| `storage.s3.endpoint` | string | Amazon S3 | Base URL of an S3-compatible service. |
| `storage.s3.path_style` | boolean | `false` | Put the bucket in the URL path rather than the host name, as most S3-compatible services need. |

### `storage.gcs`

Google Cloud Storage. Tested against the fake-gcs-server emulator only, not against Google Cloud Storage. Credentials come from Application Default Credentials.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `storage.gcs.bucket` | string | required | The bucket. It must exist. |
| `storage.gcs.endpoint` | string | Google Cloud Storage | An emulator's URL. With an endpoint set, requests carry no credentials. |

Every `endpoint` must be an `http` or `https` URL with a host and no user information, query string or fragment, so that it cannot carry a credential. The server does not start with one that breaks this rule, and its error does not repeat the value.

## `peer`

The peer listener lets any pod accept any request and forward what it cannot serve, one hop, to the pod that can. Pods authenticate each other with mutual TLS. See [Running several pods](/docs/operations/#running-several-pods).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `peer.listen` | string | required with `peer` | TCP address of the peer listener, such as `:8444`. |
| `peer.address_env` | string | required with `peer` | Name of an environment variable holding the `host:port` other pods dial to reach this pod's peer listener, for example `$(POD_IP):8444` set from the Kubernetes downward API. The server does not start if the variable is unset or holds no host. |
| `peer.cert_file` | string | required with `peer` | PEM certificate this pod presents as both peer server and peer client. It needs the `serverAuth` and `clientAuth` extended key usages and the DNS name in `server_name`. Read once, at start. |
| `peer.key_file` | string | required with `peer` | PEM key of that certificate. Read once, at start. |
| `peer.ca_file` | string | required with `peer` | PEM certificates of the CA that issues peer certificates. Use a CA that issues nothing else: any holder of a certificate it issued is treated as a pod. |
| `peer.server_name` | string | required with `peer` | The DNS name every peer certificate carries. |

The peer listener accepts TLS 1.3 only.

## Environment variables

skylite defines no environment variables of its own. The server reads:

| Variable | Read when |
|---|---|
| The one named by `peer.address_env` | `peer` is set. |
| The one named by `storage.azure.shared_key_env` | That key is set. |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_TOKEN_CREDENTIALS` and the other variables of the Azure SDK's `DefaultAzureCredential` | `storage.azure.entra` is `true`. See [Azure Blob Storage](/docs/azure-blob/#authentication). |
| The AWS SDK's variables, such as `AWS_ACCESS_KEY_ID` or `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE` | `storage.s3` is set. |
| `GOOGLE_APPLICATION_CREDENTIALS` | `storage.gcs` is set without `endpoint`. |
| `TMPDIR` | Always. The leaders' local files (write-ahead log and SQLite temporary files) and upload spool files go under it; see [Sizing](/docs/sizing/). |

## Logs

The server writes JSON log lines to standard error. Logs never hold a bearer token or any part of one, a channel token, SQL text, parameters or SQLite's messages. [Logs and shutdown](/docs/server/#logs-and-shutdown) lists what is logged.
