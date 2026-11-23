---
title: Azure Blob Storage
description: Run skylite on an Azure Blob Storage container. Account and container setup, the role the server needs, credential choices, the blob layout, and performance measured against a real account.
weight: 20
navgroup: guides
status: current
---

Azure Blob Storage is skylite's primary backend. It is tested against the Azurite emulator on every pull request, and its integration tests and the GORM acceptance suite have been run by hand against a real storage account with an Entra ID service principal. See [What has been verified](#what-has-been-verified) for the details and the gaps.

## Set up the account and container

You need a storage account and one container for skylite. The server never creates the container, and nothing else should write to it.

```sh
az storage account create -n mystorageaccount -g my-rg -l westeurope --sku Standard_LRS
az storage container create --account-name mystorageaccount -n skylite --auth-mode login
```

skylite stores every object as a block blob in a flat namespace. An account with hierarchical namespace (Data Lake Storage) enabled has not been tested. The SKU, region and redundancy are yours to choose; every write waits for the service to confirm it, so the round trip to the account's region adds directly to commit latency.

## Grant access

The server uses these Blob service operations on the container, and no others:

| Operation | Used for |
|---|---|
| Put Blob, with `If-None-Match: *` or `If-Match` | Every write: commits, manifests, the lease and the leader record. Each write is one request; the server never stages blocks. |
| Get Blob, including ranged reads | Reading pages, commits, the manifest and the lease. |
| Get Blob Properties | Checking an object's size and version. |
| List Blobs | Finding commits and compacted objects under a database. |
| Delete Blob | Removing objects that compaction has replaced. |

The built-in role **Storage Blob Data Contributor** covers all of them. **Storage Blob Data Reader** does not, because the server writes and deletes. Assign the role to the identity the server runs as, at the narrowest scope that works for you; the container scope looks like this:

```sh
az role assignment create \
  --assignee <principal-object-id> \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<subscription>/resourceGroups/my-rg/providers/Microsoft.Storage/storageAccounts/mystorageaccount/blobServices/default/containers/skylite"
```

The real-account run held the role at account scope; container scope has not been tried. Role assignments can take a few minutes to take effect, and until they do the server's requests fail with `403`.

## Authentication

Set exactly one of these in `storage.azure`:

| Setting | Credential |
|---|---|
| `"entra": true` | An Entra ID token from the Azure SDK's `DefaultAzureCredential`. Recommended: no key to store or rotate. |
| `"shared_key_file": "/path"` | The account's base64 shared key, read from a file. |
| `"shared_key_env": "NAME"` | The account's base64 shared key, read from the environment variable `NAME`. |

A shared key grants full access to the whole account, not just the container. If you use one, mount it from a secret store and keep it out of the configuration file, which the server treats as non-secret.

### Entra ID

With `entra: true`, `DefaultAzureCredential` tries these credentials in order and keeps the first that gets a token:

1. **Environment**: a service principal in `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` (or `AZURE_CLIENT_CERTIFICATE_PATH` instead of the secret).
2. **Workload identity**: the variables the Azure workload identity webhook injects into a Kubernetes pod (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_FEDERATED_TOKEN_FILE`).
3. **Managed identity**: the identity of the VM, App Service or AKS node pool; set `AZURE_CLIENT_ID` to pick a user-assigned identity.
4. **Developer logins**: the Azure CLI (`az login`), the Azure Developer CLI and Azure PowerShell.

Set `AZURE_TOKEN_CREDENTIALS` to limit the chain: `prod` tries only the first three, `dev` only the developer logins, and the name of one credential type, such as `ManagedIdentityCredential` or `WorkloadIdentityCredential`, tries only that one. In production, setting it makes start-up failures easier to read and stops a pod from silently using a developer login.

Only the service principal with a client secret has been tested against a real account. Workload identity and managed identity have not.

### HTTPS

The default endpoint is `https://<account>.blob.core.windows.net`. With `entra: true`, a custom `endpoint` must also be `https`: the Azure SDK refuses to send a bearer token over plain HTTP. A shared key works over `http`, which is how the tests reach Azurite. The server never follows a redirect from the storage endpoint.

## Configure the server

```json
{
  "storage": {
    "prefix": "prod",
    "azure": {"account": "mystorageaccount", "container": "skylite", "entra": true}
  },
  "databases": ["orders"]
}
```

The rest of the file (listener, TLS, authentication) is in the [configuration reference](/docs/configuration/). To try the backend without an Azure account, point it at [Azurite](https://github.com/Azure/Azurite) with Azurite's published development key:

```sh
export AZURITE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
```

```json
"azure": {
  "account": "devstoreaccount1",
  "container": "skylite",
  "endpoint": "http://127.0.0.1:10000/devstoreaccount1",
  "shared_key_env": "AZURITE_KEY"
}
```

## What is stored where

Each database lives under `<prefix>/<database>/` in the container, or `<database>/` with no prefix. Several servers, or several environments, can share one container under different prefixes.

| Blob | Holds |
|---|---|
| `<prefix>/<database>/manifest` | The database's current state: which compacted objects and which commits make it up. Only ever replaced with a conditional write. |
| `<prefix>/<database>/lease` | Which server leads the database, and until when. |
| `<prefix>/<database>/leader` | The leader's peer address, for request forwarding between pods. |
| `<prefix>/<database>/<id>/log/<txn>` | One commit, as uploaded before the write was acknowledged. `<id>` is the database's 32-hex-digit identifier, `<txn>` the 20-digit transaction number. |
| `<prefix>/<database>/<id>/base/<group>-<txn>-<tag>` | Compacted pages, 256 pages of 4 KiB per object. |

The leader folds the commit log into base objects in the background and deletes what the new manifest no longer needs, after a grace period of about five minutes. Do not edit, copy over or delete these blobs while a server uses them: the server relies on being the only writer.

Blob versioning, soft delete and lifecycle management policies have not been tested with skylite. A lifecycle rule that deletes or tiers these blobs would break the databases.

### If the container is deleted

Every database in it is lost. The server does not recreate the container: after the deletion, every request the server makes to storage fails, so writes fail, and so do reads that need a page the server has not cached. On the real service the deletion completes in the background: in the test run, requests still succeeded, or found no blob, for about 3 seconds after the deletion was accepted, so a write acknowledged in that window is lost with the container. skylite has no backup feature. Azure's container soft delete can restore a deleted container with its blobs, but it has not been tested with skylite.

## Performance

Measured once, on one machine against one real Azure storage account, with phases of 15 seconds. These are observations of that setup, not guarantees; your latency to the account's region decides most of it.

| Workload | Result |
|---|---|
| One writer, autocommit inserts | about 53 commits/s, p50 latency 17 ms |
| 4 to 64 concurrent writers | about 63 to 67 commits/s in total, maximum latency 131 to 342 ms |
| Reads, warm cache | about 0.6 ms each, up to about 6,700 reads/s |
| Local filesystem, one writer, for comparison | about 223 commits/s |

Writes to one database are serialized: each commit is one upload that must complete before the next writer runs, so adding writers raises total throughput only a little and mostly adds waiting. Batch related changes into one transaction where you can. Reads served from cached pages do not wait for storage. CPU use was not measured.

## What has been verified

- In CI, on every pull request: the backend's conformance and fault-injection tests and the server's database creation against Azurite 3.37.0, with shared keys from a file and from an environment variable.
- By hand against a real account, with a service principal holding Storage Blob Data Contributor on the account: the backend's integration tests (all passed) and the GORM acceptance suite on `skylite serve` with `entra: true` (all passed).
- Not tested: workload identity, managed identity, a real account's shared key, throttling by the service, HTTP/2 to the service, a proxy between the server and the service, and accounts with hierarchical namespace.

The [storage backends page](/docs/storage-backends/) has the backend's implementation details and the full test list.
