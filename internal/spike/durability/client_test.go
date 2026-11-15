package durability

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

func TestCheckLTXSize(t *testing.T) {
	if err := checkLTXSize(blockblob.MaxUploadBlobBytes); err != nil {
		t.Fatalf("file at the single Put Blob limit rejected: %v", err)
	}
	if err := checkLTXSize(blockblob.MaxUploadBlobBytes + 1); err == nil {
		t.Fatal("file one byte over the single Put Blob limit accepted")
	}
}
