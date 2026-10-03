package transfer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gotd/td/tg"
)

const testDate = 1700000000

func byteFile(size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}

// The finished file is what the caller publishes, so its name, its contents and
// its modification time all have to survive the transfer. The date matters
// because an archive of a channel is read by when things were posted, not by
// when this application happened to fetch them.
func TestRunDownloadsAFileAndStampsItWithTheMessageDate(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(3000)
	api.messages[1] = documentMessage(1, testDate, len(api.file))
	deps, pool := testDeps(api)

	dir := t.TempDir()
	var completed []FileCompletedUpdate
	var progress []ProgressUpdate
	stats, err := Run(context.Background(), deps, Options{
		Dir:        dir,
		Peer:       testPeer,
		Messages:   []int{1},
		Threads:    4,
		Tasks:      1,
		OnProgress: func(update ProgressUpdate) { progress = append(progress, update) },
		OnFileCompleted: func(update FileCompletedUpdate) {
			completed = append(completed, update)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if stats.Files != 1 || stats.Failed != 0 || stats.Deleted != 0 {
		t.Fatalf("stats are %+v", stats)
	}
	if len(completed) != 1 {
		t.Fatalf("completed %d files, want 1", len(completed))
	}
	update := completed[0]
	if update.MessageID != 1 {
		t.Fatalf("the completed file reports message %d", update.MessageID)
	}
	if want := filepath.Join(dir, "1_file-1.bin"); update.Path != want {
		t.Fatalf("the completed file is %q, want %q", update.Path, want)
	}

	// The working file has been renamed out of its temporary name, so a leftover
	// .tmp would mean the transfer stopped between writing and finishing.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("the working directory holds %v, want one finished file", names)
	}

	content, err := os.ReadFile(update.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, api.file) {
		t.Fatalf("the file holds %d bytes, want %d", len(content), len(api.file))
	}
	info, err := os.Stat(update.Path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.ModTime().Unix(); got != testDate {
		t.Fatalf("the file is stamped %d, want the message's date %d", got, testDate)
	}

	// The bytes are fetched from the datacenter the media lives on, not from the
	// account's own.
	if len(pool.dcs) == 0 || pool.dcs[len(pool.dcs)-1] != 2 {
		t.Fatalf("the transfer used datacenters %v, want the media's 2", pool.dcs)
	}

	// Progress is reported before the first byte, so the file counts as started
	// the moment work begins rather than once a chunk has landed, and the last
	// report says the file is complete.
	if len(progress) < 2 {
		t.Fatalf("progress was reported %d times", len(progress))
	}
	if progress[0].Downloaded != 0 {
		t.Fatalf("the first report claims %d bytes", progress[0].Downloaded)
	}
	if last := progress[len(progress)-1]; !last.Completed || last.Downloaded != int64(len(api.file)) {
		t.Fatalf("the last report is %+v", last)
	}
}

// One file failing must not cost the others. The engine this replaced reported
// a failed transfer as a success and renamed the partial file into place, which
// is how a task could show a completed file of zero bytes.
func TestRunKeepsGoingWhenOneTransferFails(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(2000)
	for _, id := range []int{1, 2, 3} {
		api.messages[id] = documentMessage(id, testDate, len(api.file))
	}
	// The middle file's transfer is refused, as a file whose reference expired
	// would be.
	api.failFileFor[2000] = true
	deps, _ := testDeps(api)

	dir := t.TempDir()
	var completed []FileCompletedUpdate
	stats, err := Run(context.Background(), deps, Options{
		Dir:      dir,
		Peer:     testPeer,
		Messages: []int{1, 2, 3},
		Threads:  2,
		Tasks:    1,
		OnFileCompleted: func(update FileCompletedUpdate) {
			completed = append(completed, update)
		},
	})
	if err != nil {
		t.Fatalf("a failed transfer stopped the batch: %v", err)
	}

	if stats.Files != 3 || stats.Failed != 1 {
		t.Fatalf("stats are %+v, want three files and one failure", stats)
	}
	if len(completed) != 2 {
		t.Fatalf("%d files were published, want the two that succeeded", len(completed))
	}
	// The failed file leaves nothing behind: neither a half-written working file
	// nor a finished one.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "2_") {
			t.Fatalf("the failed transfer left %q behind", entry.Name())
		}
	}
	if len(entries) != 2 {
		t.Fatalf("the working directory holds %d entries, want 2", len(entries))
	}
}

// A post deleted between being indexed and being read, and a message that never
// held anything downloadable, are both ordinary. Neither is a failure: they are
// counted and the batch continues.
func TestRunSkipsDeletedAndMediaLessMessages(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(1500)
	api.messages[1] = documentMessage(1, testDate, len(api.file))
	api.missing[2] = true
	api.messages[3] = &tg.Message{ID: 3, Date: testDate}
	deps, _ := testDeps(api)

	var completed int
	stats, err := Run(context.Background(), deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: []int{1, 2, 3},
		Threads:  2,
		Tasks:    1,
		OnFileCompleted: func(FileCompletedUpdate) {
			completed++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Files != 1 || stats.Deleted != 1 || stats.Empty != 1 || stats.Failed != 0 {
		t.Fatalf("stats are %+v", stats)
	}
	if completed != 1 {
		t.Fatalf("%d files were published, want 1", completed)
	}
}

// A file reference has a lifetime, and Telegram retires it. Its own clients
// answer that by reading the message the file came from and trying again - they
// keep a table of exactly that mapping - and this engine does the same, so one
// stale file costs one read rather than a failed task that re-reads every
// message of the batch.
func TestRunRenewsAnExpiredFileReference(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(4000)
	api.messages[1] = documentMessage(1, testDate, len(api.file))
	// The first byte request is refused as expired; the ones after it are not.
	api.staleOnce[1000] = true
	deps, _ := testDeps(api)

	dir := t.TempDir()
	var completed []FileCompletedUpdate
	stats, err := Run(context.Background(), deps, Options{
		Dir:      dir,
		Peer:     testPeer,
		Messages: []int{1},
		Threads:  1,
		Tasks:    1,
		OnFileCompleted: func(update FileCompletedUpdate) {
			completed = append(completed, update)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if stats.Failed != 0 {
		t.Fatalf("an expired reference was reported as a failure: %+v", stats)
	}
	if stats.Refreshed != 1 {
		t.Fatalf("%d files were repaired, want 1 (%+v)", stats.Refreshed, stats)
	}
	if len(completed) != 1 {
		t.Fatalf("%d files were published, want 1", len(completed))
	}
	// The window the message belongs to is read again - one request, the batch
	// form - and the file itself is retried rather than abandoned. A refresh
	// that fell back to reading the message on its own would show up here as a
	// MessagesGetHistoryRequest.
	if got := api.count("MessagesGetMessagesRequest"); got != 2 {
		t.Fatalf("the run made %d batch reads, want 2: one for the window, one to renew it (all: %v)", got, api.names())
	}
	if got := api.count("MessagesGetHistoryRequest"); got != 0 {
		t.Fatalf("the refresh read a single message instead of the window (all: %v)", api.names())
	}
	if got := api.count("UploadGetFileRequest"); got < 2 {
		// One refused, then the retry. A transfer that gave up would have asked
		// exactly once.
		t.Fatalf("the file was requested %d times, want the retry to have happened", got)
	}
	if content, err := os.ReadFile(completed[0].Path); err != nil || !bytes.Equal(content, api.file) {
		t.Fatalf("the repaired file is wrong: %v", err)
	}
}

// A reference that cannot be renewed is still a failure - the engine must not
// report a file it never fetched.
func TestRunFailsAFileWhoseReferenceCannotBeRenewed(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(4000)
	api.messages[1] = documentMessage(1, testDate, len(api.file))
	// Every request is refused as expired, so the refresh cannot help.
	api.failFileFor[1000] = true
	deps, _ := testDeps(api)

	var completed int
	stats, err := Run(context.Background(), deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: []int{1},
		Threads:  1,
		Tasks:    1,
		OnFileCompleted: func(FileCompletedUpdate) {
			completed++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 || completed != 0 {
		t.Fatalf("stats are %+v with %d published, want one failure and none published", stats, completed)
	}
}

// The whole point of the batched reader: the request count follows the batch,
// not the message count. This is the number that made pacing the account's
// metadata traffic possible at all.
func TestRunReadsABatchOfMessagesInAHandfulOfRequests(t *testing.T) {
	ids := idsFrom(1, 250)
	api := newFakeAPI()
	api.file = byteFile(0)
	fillMessages(api, ids)
	deps, _ := testDeps(api)

	stats, err := Run(context.Background(), deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: ids,
		Threads:  1,
		Tasks:    2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 250 ids in windows of 100 is three reads, plus the one peer resolution the
	// dialog costs.
	if stats.BatchCalls != 3 {
		t.Fatalf("250 messages cost %d batch reads, want 3", stats.BatchCalls)
	}
	if stats.SingleCalls != 0 {
		t.Fatalf("%d messages were read one at a time, want 0", stats.SingleCalls)
	}
	if stats.Files != 250 {
		t.Fatalf("%d files were transferred, want 250", stats.Files)
	}
}

// A batch with no peer cannot decide where anything goes, and says so rather
// than transferring into a directory nobody will look in.
func TestRunRefusesABatchWithoutAPeer(t *testing.T) {
	api := newFakeAPI()
	deps, _ := testDeps(api)
	if _, err := Run(context.Background(), deps, Options{Dir: t.TempDir(), Messages: []int{1}}); err == nil {
		t.Fatal("a batch without a peer was accepted")
	}
}

// A cancellation reaches the transfers and stops them, rather than leaving work
// running against a directory the caller is about to remove.
func TestRunStopsWhenCancelled(t *testing.T) {
	ids := idsFrom(1, 40)
	api := newFakeAPI()
	api.file = byteFile(4096)
	fillMessages(api, ids)
	deps, _ := testDeps(api)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: ids,
		Threads:  1,
		Tasks:    1,
	}); err == nil {
		t.Fatal("a cancelled batch reported success")
	}
}

// The working name is internal - the caller chooses the published one - so it
// only has to be unique and safe: one path component, no separator, and short
// enough for a filesystem to accept.
func TestTempNameIsSafeAndBounded(t *testing.T) {
	cases := []struct {
		label string
		file  string
		want  string
	}{
		{"reserved characters are replaced", `a:b*c?d"e<f>g|h`, "1_a_b_c_d_e_f_g_h.tmp"},
		{"a trailing dot is dropped", "archive.", "1_archive.tmp"},
		{"leading dots are dropped too", "..hidden", "1_hidden.tmp"},
		{"a name of only dots still resolves", ".", "1_file.tmp"},
		{"an unprintable name still resolves", "\x00\x01", "1___.tmp"},
	}
	for _, c := range cases {
		if got := tempName(1, c.file); got != c.want {
			t.Errorf("%s: tempName is %q, want %q", c.label, got, c.want)
		}
	}

	// The property that matters is not the exact spelling but that a hostile
	// name cannot become a path. Whatever the input, the answer is one component
	// that stays inside the directory it is joined to.
	for _, hostile := range []string{"../../etc/passwd", `/etc/passwd`, `..\..\windows`, "a/../../b", "//"} {
		name := tempName(1, hostile)
		if strings.ContainsAny(name, `/\`) {
			t.Errorf("%q produced %q, which is a path rather than a name", hostile, name)
		}
		if filepath.Dir(filepath.Join("/downloads/job-1", name)) != "/downloads/job-1" {
			t.Errorf("%q escaped its directory: %q", hostile, name)
		}
	}

	long := strings.Repeat("é", 400)
	name := tempName(7, long)
	if len(name) > 255 {
		t.Fatalf("a long name produced %d bytes, which no filesystem accepts", len(name))
	}
	if !strings.HasSuffix(name, tempExt) || !strings.HasPrefix(name, "7_") {
		t.Fatalf("a trimmed name lost its identity: %q", name)
	}
	// Trimmed on a rune boundary, so the name stays valid UTF-8: cutting a
	// multi-byte rune in half would produce a name the filesystem stores as
	// something else.
	if !utf8.ValidString(name) {
		t.Fatalf("a trimmed name is not valid UTF-8: %q", name)
	}
}
