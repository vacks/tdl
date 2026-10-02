package download

import (
	"testing"
	"time"
)

// The ingest path adopts a file a message task finished but never claimed.
//
// It is the one statement in that transaction that writes from a values list
// built at call time, so it is the one whose placeholders can be numbered
// differently from the order the arguments are written in. The count of them is
// right and the statement compiles either way; what it does instead is write the
// task id into a timestamp column, or refuse the row as text where a message id
// belongs. Both leave a message task's finished file unadopted, which is a
// second download of the same media rather than a visible failure.
//
// The pipeline test next door passes an empty map, so this statement never ran
// there.
func TestPostgresAdoptingAMessageTasksFileWritesTheOwnershipRow(t *testing.T) {
	m := openChatItemTestManager(t, "adopt-chat", ChatStatusScanning, nil)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	adopted := map[mediaClaimKey]string{
		{dialogKey: "channel:adopt", messageID: 11}: "/downloads/adopt/11.bin",
		{dialogKey: "channel:adopt", messageID: 12}: "/downloads/adopt/12.bin",
	}
	tx, err := m.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := adoptMessageTaskFiles(tx, adopted, now); err != nil {
		t.Fatalf("adopting a finished message task's files: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for key := range adopted {
		var status, ownerKind, ownerID, updatedAt string
		if err := m.db.QueryRow(`SELECT status, owner_kind, owner_id, updated_at FROM downloaded_media WHERE dialog_key = ? AND message_id = ?`, key.dialogKey, key.messageID).
			Scan(&status, &ownerKind, &ownerID, &updatedAt); err != nil {
			t.Fatalf("message %d was not adopted: %v", key.messageID, err)
		}
		if status != "completed" || ownerKind != "message" {
			t.Errorf("message %d was adopted as %s/%s", key.messageID, status, ownerKind)
		}
		// The timestamp is the column the argument order can put a task id in.
		if _, err := time.Parse(time.RFC3339Nano, updatedAt); err != nil {
			t.Errorf("message %d carries %q as its updated_at, which is not a timestamp: "+
				"the values list and the select list are numbered in different orders", key.messageID, updatedAt)
		}
		if ownerID != "" {
			t.Errorf("message %d was adopted by %q, want the record the message task never wrote", key.messageID, ownerID)
		}
	}
}
