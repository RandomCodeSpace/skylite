package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

type Visit struct {
	ID     uint
	Writer int
	N      int
}

// TestConcurrentWriters runs GORM transactions from more writers than the
// server lets wait for the write lock, 16 per principal: those it refuses
// with busy, which ran nothing, the driver sends again after a pause, so
// every transaction commits and none reports an error.
func TestConcurrentWriters(t *testing.T) {
	const writers, each = 64, 4
	db := gormDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(writers)
	if err := db.AutoMigrate(&Visit{}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers*each)
	for w := range writers {
		wg.Go(func() {
			for n := range each {
				errs <- db.Transaction(func(tx *gorm.DB) error {
					var before int64
					if err := tx.Model(&Visit{}).Where("writer = ?", w).Count(&before).Error; err != nil {
						return err
					}
					if before != int64(n) {
						return fmt.Errorf("writer %d sees %d visits before its visit %d", w, before, n)
					}
					return tx.Create(&Visit{Writer: w, N: n}).Error
				})
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var n int64
	if err := db.Model(&Visit{}).Count(&n).Error; err != nil || n != writers*each {
		t.Fatalf("%d visits, %v; want %d", n, err, writers*each)
	}
}

// TestReadOnlyTransactions runs a GORM transaction with ReadOnly set: it
// reads one snapshot, which a write committed meanwhile does not change,
// and its own write fails with SQLITE_READONLY.
func TestReadOnlyTransactions(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	if err := db.Create(&Tag{Name: "ro-before"}).Error; err != nil {
		t.Fatal(err)
	}
	probe, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx := db.WithContext(probe).Begin(&sql.TxOptions{ReadOnly: true})
	var se *skylite.Error
	if errors.As(tx.Error, &se) && se.Code == skylite.CodeNotServing {
		t.Skip("the server serves no read-only channels")
	}
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	_ = tx.Rollback()
	ro := &sql.TxOptions{ReadOnly: true}
	if err := db.Transaction(func(tx *gorm.DB) error {
		var first, second int64
		if err := tx.Model(&Tag{}).Count(&first).Error; err != nil {
			return err
		}
		if err := db.Create(&Tag{Name: "ro-during"}).Error; err != nil { // outside the transaction
			return err
		}
		if err := tx.Model(&Tag{}).Count(&second).Error; err != nil {
			return err
		}
		if first != second {
			return fmt.Errorf("the snapshot moved from %d to %d tags", first, second)
		}
		err := tx.Create(&Tag{Name: "ro-inside"}).Error
		if !errors.As(err, &se) || se.Code != skylite.CodeSQLError || se.SQLiteCode != 8 { // SQLITE_READONLY
			return fmt.Errorf("a write in a read-only transaction: %w; want SQLITE_READONLY", err)
		}
		return nil
	}, ro); err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := db.Model(&Tag{}).Where("name IN ?", []string{"ro-during", "ro-inside"}).Pluck("name", &names).Error; err != nil || len(names) != 1 || names[0] != "ro-during" {
		t.Fatalf("tags %v, %v; want the write outside the transaction only", names, err)
	}
}
