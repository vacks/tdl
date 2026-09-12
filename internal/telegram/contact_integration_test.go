//go:build integration

package telegram

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/storage"
)

// TestIntegrationSearchContact performs no message reads or writes. It is
// deliberately opt-in because it uses a copied, already-authorized Telegram
// session and makes one contacts.search request against the configured test
// account. Keeping the account data outside the production directory lets a
// live application continue running without competing for session state.
func TestIntegrationSearchContact(t *testing.T) {
	dataDir := strings.TrimSpace(os.Getenv("TDL_INTEGRATION_DATA_DIR"))
	query := strings.TrimSpace(os.Getenv("TDL_CONTACT_QUERY"))
	if dataDir == "" || query == "" {
		t.Skip("set TDL_INTEGRATION_DATA_DIR and TDL_CONTACT_QUERY to run Telegram contact integration test")
	}
	manager, err := Open(dataDir, func() string { return strings.TrimSpace(os.Getenv("TDL_INTEGRATION_PROXY")) })
	if err != nil {
		t.Fatalf("open copied Telegram account data: %v", err)
	}
	defer manager.Stop()
	accountID, err := manager.CurrentID()
	if err != nil {
		t.Fatalf("current authorized account: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var result *tg.ContactsFound
	err = manager.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		found, err := client.API().ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: query, Limit: 20})
		if err != nil {
			return err
		}
		result = found
		return nil
	})
	if err != nil {
		t.Fatalf("Telegram contacts.search: %v", err)
	}
	if result == nil || (len(result.MyResults) == 0 && len(result.Results) == 0) {
		t.Fatalf("no Telegram contact or username matched the supplied query")
	}
	// Do not log contact names, usernames, phone numbers, or IDs. The count is
	// sufficient to prove that this user-provided selector is resolvable.
	t.Logf("contacts.search matched %d result(s)", len(result.MyResults)+len(result.Results))
}
