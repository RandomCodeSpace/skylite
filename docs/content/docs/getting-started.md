---
title: Quick start
description: Download a release, verify it, run the server on a local directory, and connect to it from Go and from Java.
weight: 10
navgroup: guides
status: current
---

This guide runs one server on your machine, with its databases in a local directory and plain HTTP on `127.0.0.1`. That is for trying skylite out only: a local directory is as durable as the disk under it. For a real deployment, continue with [Azure Blob Storage](/docs/azure-blob/) and [Running in production](/docs/operations/).

You need Linux or macOS, `curl` and `openssl`. The Go example needs Go 1.26 or later; the Java example needs Java 21 or later and Maven.

## 1. Download and verify a release

Each [release](https://github.com/RandomCodeSpace/skylite/releases) has a `tar.gz` archive for Linux and macOS on amd64 and arm64, a `SHA256SUMS` file, an SBOM for each archive, and a [Cosign](https://docs.sigstore.dev/) signature of `SHA256SUMS`. No container image is published.

```sh
VERSION=0.0.3
gh release download "v$VERSION" -R RandomCodeSpace/skylite \
  -p "skylite_${VERSION}_linux_amd64.tar.gz" -p 'SHA256SUMS*'
```

Check that `SHA256SUMS` was signed by this repository's release workflow for that tag, then check the archive against it:

```sh
cosign verify-blob \
  --certificate SHA256SUMS.pem --signature SHA256SUMS.sig \
  --certificate-identity "https://github.com/RandomCodeSpace/skylite/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
```

On macOS, use `shasum -a 256 --ignore-missing -c SHA256SUMS`. Then unpack the binary:

```sh
tar -xzf "skylite_${VERSION}_linux_amd64.tar.gz"
./skylite version
```

With a Go toolchain you can instead run `go install github.com/RandomCodeSpace/skylite/cmd/skylite@v0.0.3`, or build from a clone with `make build`, which writes `bin/skylite`.

## 2. Make a signing key and a token

Every request needs a JWT bearer token from an issuer the server trusts. In production that is your identity provider, such as Kubernetes service account tokens. For this trial, make a key pair and sign a token with `openssl`:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out issuer.key
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
N=$(openssl rsa -in issuer.key -noout -modulus | cut -d= -f2 | xxd -r -p | b64url)
printf '{"keys":[{"kty":"RSA","kid":"local","use":"sig","alg":"RS256","n":"%s","e":"AQAB"}]}\n' "$N" > jwks.json

NOW=$(date +%s)
HEADER=$(printf '{"alg":"RS256","typ":"JWT","kid":"local"}' | b64url)
PAYLOAD=$(printf '{"iss":"https://issuer.example","aud":"skylite","sub":"me","iat":%d,"exp":%d}' "$NOW" $((NOW + 3600)) | b64url)
SIG=$(printf '%s.%s' "$HEADER" "$PAYLOAD" | openssl dgst -sha256 -sign issuer.key | b64url)
printf '%s.%s.%s\n' "$HEADER" "$PAYLOAD" "$SIG" > token
```

`jwks.json` holds the public key the server verifies with; `token` is valid for an hour, for the subject `me`.

## 3. Run the server

```sh
cat > skylite.json <<'EOF'
{
  "listen": "127.0.0.1:8080",
  "plaintext": true,
  "auth": {
    "issuers": [{"issuer": "https://issuer.example", "audience": "skylite", "jwks_file": "jwks.json"}],
    "rules": [{"issuer": "https://issuer.example", "subject": "me", "database": "*", "role": "admin"}]
  },
  "storage": {"localfs": {"path": "data"}},
  "databases": ["orders"]
}
EOF
mkdir -p data
./skylite serve -config skylite.json
```

The server logs JSON lines to standard error. Until the database exists it logs failed candidacies for `orders`; that is expected. In another terminal, create the database:

```sh
curl -s -X PUT -H "Authorization: Bearer $(cat token)" -H 'Content-Type: application/json' \
  --data '{}' http://127.0.0.1:8080/v1/databases/orders
```

The answer is `{"done":{"txn":1,"epoch":1}}`. The server takes office for the new database within about 20 seconds by default (a third of `lease_ttl`); until then, requests for it get `503` `not_serving`. Each field of the configuration file is described in the [configuration reference](/docs/configuration/).

## 4. Connect from Go

The Go driver is the package `github.com/RandomCodeSpace/skylite/driver` in the main module:

```sh
go get github.com/RandomCodeSpace/skylite@v0.0.3
```

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

func main() {
	c, err := skylite.NewConnector(skylite.Config{
		Endpoint:       "http://127.0.0.1:8080",
		AllowPlaintext: true, // local trial only; use https in production
		Database:       "orders",
		Token: skylite.TokenFunc(func(ctx context.Context) (string, error) {
			b, err := os.ReadFile("token")
			return strings.TrimSpace(string(b)), err
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO items (name) VALUES (?)", "first"); err != nil {
		log.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Println("items:", n)
}
```

Run it from the directory that holds `token`. The [Go driver guide](/docs/go-driver/) covers GORM, transactions and error handling.

## 5. Connect from Java

The JDBC driver is not published to a Maven repository. Build and install it into your local repository from a clone:

```sh
git clone https://github.com/RandomCodeSpace/skylite.git
mvn -q -f skylite/clients/java/driver/pom.xml install -DskipTests
```

Then depend on `io.github.randomcodespace:skylite-jdbc:0.0.0-SNAPSHOT`, or put its jar and its one dependency, `jackson-core` 3, on the class path:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty("tokenFile", "token");
        p.setProperty("allowPlaintext", "true"); // local trial only
        try (Connection c = DriverManager.getConnection("jdbc:skylite:http://127.0.0.1:8080/orders", p);
                Statement s = c.createStatement()) {
            s.executeUpdate("INSERT INTO items (name) VALUES ('from java')");
            try (ResultSet rs = s.executeQuery("SELECT id, name FROM items ORDER BY id")) {
                while (rs.next()) {
                    System.out.println(rs.getLong("id") + " " + rs.getString("name"));
                }
            }
        }
    }
}
```

The [JDBC driver guide](/docs/jdbc-driver/) covers Spring Boot, Hibernate, transactions and error handling.

## Next steps

- [Azure Blob Storage](/docs/azure-blob/): move the databases to a real object store.
- [Sizing](/docs/sizing/) and [Running in production](/docs/operations/): resources, several pods, failover and the errors clients must handle.
- [Configuration reference](/docs/configuration/): TLS, issuers and access rules.
