package topics_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// drive runs cmd to completion synchronously and routes any resulting
// messages back through the Model. Cmds that don't deliver a value within
// driveCmdDeadline are dropped — that's how we skip [tea.Tick] cmds (the
// auto-refresh chain) without blocking on real timers.
func drive(t *testing.T, m *topics.Model, cmd tea.Cmd) {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		if next == nil {
			continue
		}
		msg := runCmdNonBlocking(next)
		if msg == nil {
			continue
		}
		if batch, ok := msg.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

const driveCmdDeadline = 50 * time.Millisecond

func runCmdNonBlocking(cmd tea.Cmd) tea.Msg {
	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()
	select {
	case msg := <-done:
		return msg
	case <-time.After(driveCmdDeadline):
		return nil
	}
}

func TestNew_DefaultColumns(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	out := m.View()
	assert.Contains(t, out, "Name")
	assert.Contains(t, out, "Partitions")
	assert.Contains(t, out, "Replicas")
	assert.Contains(t, out, "Cleanup")
	assert.Contains(t, out, "Messages")
	assert.Contains(t, out, "Size")
}

func TestTopics_BreadcrumbIsEmpty(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha", Partitions: 1, Replicas: 1},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	assert.Empty(t, m.Breadcrumb())
}

func TestInit_LoadsTopicsAndShowsCounter(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha", Partitions: 1, Replicas: 1},
		{Name: "beta", Partitions: 3, Replicas: 1},
	}, nil)
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())

	assert.Len(t, m.AllTopics(), 2)
	out := m.View()
	// the count moved to the frame title, the body only renders the table.
	assert.Contains(t, m.Title(), "Topics [2]")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "beta")
}

func TestInit_ErrorRaisesToast(t *testing.T) {
	svc := newFakeService(nil, errors.New("connection refused"))
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "connection refused")
}

func TestACLDenial_RendersMarkerInsteadOfDash(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "secret", Partitions: 1, Replicas: 1},
		{Name: "open", Partitions: 1, Replicas: 1},
	}, nil)
	svc.configErrs = map[string]error{"secret": kerr.TopicAuthorizationFailed}
	svc.sizeErrs = map[string]error{"secret": kerr.TopicAuthorizationFailed}
	m := topics.New(topics.Options{
		Service: svc,
		Columns: []string{"name", "cleanup_policy", "size"},
	})

	drive(t, m, m.Init())

	out := m.View()
	secretLine := lineWithPrefix(t, out, "secret")
	openLine := lineWithPrefix(t, out, "open")
	assert.Contains(t, secretLine, "⊘", "ACL-denied cells must render the denial marker, not a plain dash")
	assert.NotContains(t, openLine, "⊘", "topics without denials must keep their normal rendering")
}

func TestACLDenial_AggregatedWarnToast(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "a", Partitions: 1, Replicas: 1},
		{Name: "b", Partitions: 1, Replicas: 1},
	}, nil)
	svc.configErrs = map[string]error{
		"a": kerr.TopicAuthorizationFailed,
		"b": kerr.TopicAuthorizationFailed,
	}
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())

	got := findToast(t, m, "configs:")
	assert.Contains(t, got, "2 topics denied (ACL)")
	assert.Contains(t, got, "a, b")
}

func TestACLDenial_DedupAcrossLoads(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "a", Partitions: 1, Replicas: 1},
	}, nil)
	svc.configErrs = map[string]error{"a": kerr.TopicAuthorizationFailed}
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())
	initialDenialToasts := countToasts(m, "denied")
	require.Equal(t, 1, initialDenialToasts, "first load surfaces the denial once")

	drive(t, m, m.HandleRefreshTick())

	assert.Equal(t, 1, countToasts(m, "denied"),
		"the second tick observes the same denial set and must stay silent")
}

func lineWithPrefix(t *testing.T, out, prefix string) string {
	t.Helper()
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, prefix) {
			return line
		}
	}
	t.Fatalf("no rendered line contained %q", prefix)
	return ""
}

func findToast(t *testing.T, m *topics.Model, needle string) string {
	t.Helper()
	for _, item := range m.Toasts().Items() {
		if strings.Contains(item.Message, needle) {
			return item.Message
		}
	}
	t.Fatalf("no toast contained %q", needle)
	return ""
}

func countToasts(m *topics.Model, needle string) int {
	n := 0
	for _, item := range m.Toasts().Items() {
		if strings.Contains(item.Message, needle) {
			n++
		}
	}
	return n
}

func TestInternalToggle_HidesAndShowsInternalTopics(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "user_events"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "__transaction_state", IsInternal: true},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	// internal hidden by default
	assert.False(t, m.ShowInternal())
	visible := m.Topics()
	require.Len(t, visible, 1)
	assert.Equal(t, "user_events", visible[0].Name)
	assert.Equal(t, 2, m.HiddenInternalCount())

	// toggle: now visible
	_ = m.Update(keyPress("i"))
	assert.True(t, m.ShowInternal())
	visible = m.Topics()
	assert.Len(t, visible, 3)
	out := m.View()
	assert.Contains(t, out, "__consumer_offsets")
}

func TestTitle_ShowsHiddenCount(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "topic-1"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "__transaction_state", IsInternal: true},
		{Name: "__schemas", IsInternal: true},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	// hidden-internal counter lives in the frame title now.
	assert.Contains(t, m.Title(), "Topics [1, +3 internal hidden]")
}

func TestEnter_RaisesMessagesAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{
		{Name: "alpha"}, {Name: "beta"},
	})
	_ = m.Update(keyPress("enter"))
	assert.Equal(t, "alpha", m.ConsumeAction().Messages)
}

func TestM_AlsoOpensMessages(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_ = m.Update(keyPress("m"))
	assert.Equal(t, "alpha", m.ConsumeAction().Messages)
}

func TestC_RaisesConfigsAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_ = m.Update(keyPress("c"))
	assert.Equal(t, "alpha", m.ConsumeAction().Configs)
}

func TestG_RaisesGroupsAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_ = m.Update(keyPress("g"))
	assert.Equal(t, "alpha", m.ConsumeAction().Groups)
}

func TestP_RaisesProduceAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_ = m.Update(keyPress("p"))
	assert.Equal(t, "alpha", m.ConsumeAction().Produce)
}

func TestEsc_RaisesQuit(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Quit)
}

func TestReadOnly_BlocksMutatingHotkeys(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc, ReadOnly: true})
	drive(t, m, m.Init())

	for _, k := range []string{"n", "ctrl+d", "y", "p"} {
		_ = m.Update(keyPress(k))
		assert.Empty(t, m.ConsumeAction().Produce, "p must not raise produce in RO")
		assert.False(t, m.ConfirmOpen(), "ctrl+d must not open confirm in RO")
		assert.Equal(t, topics.ModeList, m.CurrentMode(), "n/y must not enter overlay in RO")
		// each blocked key produces a warning toast
	}
	// at least one warning toast surfaced
	assert.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestDelete_ConfirmYesTriggersDeleteCmd(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("ctrl+d"))
	assert.True(t, m.ConfirmOpen())
	assert.Equal(t, "alpha", m.PendingTopic())

	cmd := m.Update(keyPress("y"))
	drive(t, m, cmd)
	assert.False(t, m.ConfirmOpen())
	assert.Equal(t, []string{"alpha"}, svc.Deleted())
}

func TestDelete_ConfirmNoCancels(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("ctrl+d"))
	require.True(t, m.ConfirmOpen())

	_ = m.Update(keyPress("n"))
	assert.False(t, m.ConfirmOpen())
	assert.Empty(t, svc.Deleted())
}

func TestN_OpensCreateForm(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	assert.Equal(t, topics.ModeCreate, m.CurrentMode())
	out := m.View()
	assert.Contains(t, out, "New topic")
	// "create" is the save-verb label in the create-form hint footer
	// and appears nowhere else in the form output, so this assertion
	// pins that the footer is rendered.
	assert.Contains(t, out, "create")
}

func TestCreateForm_PasteLandsInFocusedField(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	require.Equal(t, topics.ModeCreate, m.CurrentMode())
	m.CreateForm().Form().FocusKey("name")

	_ = m.Update(tea.PasteMsg{Content: "orders.events"})

	got, _ := m.CreateForm().Form().Field("name")
	assert.Equal(t, "orders.events", got.Value, "paste must reach the create form name field")
	assert.Equal(t, topics.FormNormal, m.CreateForm().Mode(), "paste must not cross into INSERT")
}

func TestCloneForm_PasteLandsAndDropsWhileConfirmOpen(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("y"))
	require.Equal(t, topics.ModeClone, m.CurrentMode())
	m.CloneForm().Form().FocusKey("destination")

	_ = m.Update(tea.PasteMsg{Content: "-eu"})
	dst, _ := m.CloneForm().Form().Field("destination")
	require.Contains(t, dst.Value, "-eu", "paste must reach the clone form destination field")
	before := dst.Value

	// open the clone confirm: it owns input, so a paste must be dropped
	// rather than leak into the field behind the modal.
	_ = m.Update(keyPress("s"))
	_ = m.Update(tea.PasteMsg{Content: "LEAK"})

	after, _ := m.CloneForm().Form().Field("destination")
	assert.Equal(t, before, after.Value, "paste must not mutate the field while the clone confirm owns input")
}

func TestCreateForm_EscReturnsToList(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	require.Equal(t, topics.ModeCreate, m.CurrentMode())
	_ = m.Update(keyPress("esc"))
	assert.Equal(t, topics.ModeList, m.CurrentMode())
}

func TestWantsRawInput_TracksFormModes(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	assert.False(t, m.WantsRawInput(), "list mode does not edit text")

	// open the create form — it starts in NORMAL, so raw-input must
	// stay off (so global shortcuts like `?` keep working).
	_ = m.Update(keyPress("n"))
	require.Equal(t, topics.ModeCreate, m.CurrentMode())
	assert.False(t, m.WantsRawInput(), "create form in NORMAL is not raw")

	// enter INSERT — typing into the name field; raw-input takes over.
	_ = m.Update(keyPress("enter"))
	assert.True(t, m.WantsRawInput(), "create form in INSERT is raw")

	// esc out of INSERT (back to form NORMAL) → raw-input lifts.
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.WantsRawInput(), "leaving INSERT lifts raw-input")

	// esc again closes the create overlay; back at the list.
	_ = m.Update(keyPress("esc"))
	require.Equal(t, topics.ModeList, m.CurrentMode())
	assert.False(t, m.WantsRawInput())

	_ = m.Update(keyPress("y"))
	require.Equal(t, topics.ModeClone, m.CurrentMode())
	assert.False(t, m.WantsRawInput(), "clone form starts in NORMAL")
}

func TestCreateForm_CtrlUClearsFormInNormal(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	_ = m.Update(keyPress("enter"))
	for _, r := range "abcd" {
		_ = m.Update(keyPressRune(r))
	}
	// drop back to NORMAL so ctrl+u is interpreted at form level rather than
	// as the field-level readline kill.
	_ = m.Update(keyPress("esc"))

	_ = m.Update(keyPress("ctrl+u"))

	got, _ := m.CreateForm().Form().Field("name")
	assert.Empty(t, got.Value, "name should reset to default after ctrl+u")
	// partitions/replication_factor have non-empty defaults — they must be
	// restored, not just cleared, so the form is genuinely back at start.
	parts, _ := m.CreateForm().Form().Field("partitions")
	assert.Equal(t, "1", parts.Value)
}

func TestCreateForm_CtrlUNoopWhilePopupOpen(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	// fill in name, leave to NORMAL.
	_ = m.Update(keyPress("enter"))
	for _, r := range "orders" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("esc"))
	// focus the cleanup_policy segmented field and open its popup via enter.
	m.CreateForm().Form().FocusKey("cleanup_policy")
	_ = m.Update(keyPress("enter"))
	require.True(t, m.CreateForm().Form().PopupActive(), "popup must be open before the assertion")

	_ = m.Update(keyPress("ctrl+u"))

	// popup is a modal sub-state; ctrl+u must yield to it instead of
	// wiping every field the user already filled in.
	got, _ := m.CreateForm().Form().Field("name")
	assert.Equal(t, "orders", got.Value, "name must survive ctrl+u while a popup is open")
	assert.True(t, m.CreateForm().Form().PopupActive(), "popup must stay open")
}

func TestCreateForm_SValidatesAndDispatches(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))

	// form opens in NORMAL — press enter to start typing into the focused
	// `name` field, then fill it in.
	_ = m.Update(keyPress("enter"))
	for _, r := range "orders" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("esc")) // back to NORMAL so `s` is a binding
	cmd := m.Update(keyPress("s"))
	drive(t, m, cmd)

	created := svc.Created()
	require.Len(t, created, 1)
	assert.Equal(t, "orders", created[0].Name)
	assert.Equal(t, int32(1), created[0].Partitions)
	assert.Equal(t, int16(1), created[0].ReplicationFactor)
}

func TestCreateForm_SWithEmptyNameShowsInlineError(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	// no name typed
	cmd := m.Update(keyPress("s"))
	drive(t, m, cmd)

	require.Equal(t, topics.ModeCreate, m.CurrentMode(), "stay on form when invalid")
	assert.Contains(t, m.CreateForm().Err(), "name")
	assert.Empty(t, svc.Created())
}

func TestCreateForm_SInInsertIsLiteral(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("n"))
	_ = m.Update(keyPress("enter")) // INSERT on `name`

	_ = m.Update(keyPressRune('s'))

	got, _ := m.CreateForm().Form().Field("name")
	assert.Equal(t, "s", got.Value, "s in INSERT must be typed, not submit the form")
	assert.Empty(t, svc.Created())
}

func TestY_OpensCloneForm(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("y"))
	assert.Equal(t, topics.ModeClone, m.CurrentMode())
	out := m.View()
	assert.Contains(t, out, "Clone topic")
	assert.Contains(t, out, "alpha")
}

func TestCloneForm_SAndYStartsCloneAndShowsProgress(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("y"))
	require.Equal(t, topics.ModeClone, m.CurrentMode())

	_ = m.Update(keyPress("s"))
	cmd := m.Update(keyPress("y")) // confirm the clone
	drive(t, m, cmd)

	cloned := svc.Cloned()
	require.Len(t, cloned, 1)
	assert.Equal(t, "alpha", cloned[0].src)
	assert.Equal(t, "alpha-clone", cloned[0].dst)
	// after the clone progress channel closes, mode returns to list
	assert.Equal(t, topics.ModeList, m.CurrentMode())
}

func TestCloneForm_ConfirmNoDismissesWithoutCloning(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("y"))
	_ = m.Update(keyPress("s"))
	_ = m.Update(keyPress("n"))

	assert.Equal(t, topics.ModeClone, m.CurrentMode(), "dismissing the confirm keeps the user on the clone form")
	assert.Empty(t, svc.Cloned(), "dismissing must not start the clone")
}

func TestCloneForm_PartialProgressRendersOverlayAndEscReturns(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	// emit one in-flight progress, then a Done so the test doesn't wait on
	// a real Kafka clone. We keep ourselves in the in-flight render via the
	// first, then assert on it before letting the next msg pump close out.
	svc.clonePartial = []kafka.CloneProgress{
		{Total: 100, Copied: 30},
		{Total: 100, Copied: 100, Done: true},
	}
	m := topics.New(topics.Options{Service: svc})
	m.SetSize(120, 30)
	drive(t, m, m.Init())

	_ = m.Update(keyPress("y"))
	_ = m.Update(keyPress("s"))
	cmd := m.Update(keyPress("y")) // confirm the clone — returns the start cmd
	// pump the cloneStartedMsg so we land on the first clonePollCmd.
	step1 := cmd()
	require.NotNil(t, step1)
	cmd = m.Update(step1)
	// the next cmd reads the first partial-progress message.
	step2 := cmd()
	_ = m.Update(step2)

	require.Equal(t, topics.ModeCloning, m.CurrentMode(), "must remain cloning before Done arrives")

	// renderCloningOverlay surfaces the labels.
	out := m.View()
	assert.Contains(t, out, "Cloning…")
	assert.Contains(t, out, "30")
	assert.Contains(t, out, "100")
	assert.Contains(t, out, "esc")

	// esc on the cloning overlay returns to the list (handleCloningKey).
	_ = m.Update(keyPress("esc"))
	assert.Equal(t, topics.ModeList, m.CurrentMode())
}

func TestRefresh_RKeyReloadsTopics(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	require.Equal(t, 1, svc.ListCalls())

	cmd := m.Update(keyPress("r"))
	drive(t, m, cmd)
	assert.Equal(t, 2, svc.ListCalls())
}

func TestSearch_FiltersTable(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "orders"},
		{Name: "events"},
		{Name: "order-history"},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	// host owns the `/` prompt now and pushes each keystroke into
	// SetSearch — exercise the screen through that public surface.
	m.SetSearch("order")
	out := m.View()
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "order-history")
	assert.NotContains(t, out, "events")
	// title surfaces the active query in k9s-style angle brackets.
	assert.Contains(t, m.Title(), "Topics [2/3] </order>")
}

func TestKeyHints_ContainExpectedLabels(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.Contains(t, got, "messages")
	assert.Contains(t, got, "configs")
	assert.Contains(t, got, "groups")
	assert.Contains(t, got, "new")
	assert.Contains(t, got, "delete")
	assert.Contains(t, got, "clone")
	assert.Contains(t, got, "produce")
}

func TestKeyHints_OmitMutatingLabelsInReadOnly(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc, ReadOnly: true})
	labels := []string{}
	for _, h := range m.KeyHints() {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.NotContains(t, got, "new")
	assert.NotContains(t, got, "delete")
	assert.NotContains(t, got, "clone")
	assert.NotContains(t, got, "produce")
}

func TestColumnConfiguration_RespectsConfig(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha", Partitions: 5, Replicas: 1},
	}, nil)
	m := topics.New(topics.Options{
		Service: svc,
		Columns: []string{"name", "partitions"},
	})
	drive(t, m, m.Init())
	out := m.View()
	assert.Contains(t, out, "Name")
	assert.Contains(t, out, "Partitions")
	assert.NotContains(t, out, "Replicas")
}

func TestSort_SCyclesSortOnCurrentColumn(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "zeta"},
		{Name: "alpha"},
		{Name: "mu"},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("s")) // first s engages asc on first sortable col (name)
	out := m.View()
	idxAlpha := strings.Index(out, "alpha")
	idxZeta := strings.Index(out, "zeta")
	require.GreaterOrEqual(t, idxAlpha, 0)
	require.GreaterOrEqual(t, idxZeta, 0)
	assert.Less(t, idxAlpha, idxZeta, "ascending sort puts alpha first")
}

func TestRefreshInterval_AutoRefreshTickIssuesCommandWhenDefaultActive(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	cmd := m.AutoRefreshTick()
	require.NotNil(t, cmd, "default non-zero interval must yield a tick cmd")
}

func TestRefreshInterval_PersistedManualYieldsNilCmd(t *testing.T) {
	svc := newFakeService(nil, nil)
	repo := &fakeRefreshRepo{loaded: 0, loadedOK: true}
	m := topics.New(topics.Options{Service: svc, RefreshIntervals: repo})
	assert.Nil(t, m.AutoRefreshTick(),
		"persisted Manual (0) must override the default and disable ticks")
}

func TestFocusTopic_CursorRestoredAfterLoad(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}, nil)
	m := topics.New(topics.Options{
		Service:    svc,
		FocusTopic: "gamma",
	})

	// act
	drive(t, m, m.Init())

	// assert
	visible := m.Topics()
	require.Len(t, visible, 3)
	assert.Equal(t, 2, m.Cursor())
}

func TestFocusTopic_UnknownTopic_CursorStaysAtZero(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
	}, nil)
	m := topics.New(topics.Options{
		Service:    svc,
		FocusTopic: "nonexistent",
	})

	// act
	drive(t, m, m.Init())

	// assert
	assert.Equal(t, 0, m.Cursor())
}

// ----- helpers -----

func buildModelWith(t *testing.T, ts []kafka.TopicSummary) *topics.Model {
	t.Helper()
	svc := newFakeService(ts, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	return m
}

type clonedPair struct {
	src, dst string
}

type alteredConfig struct {
	topic, key, value string
}

type fakeService struct {
	mu       sync.Mutex
	topics   []kafka.TopicSummary
	listErr  error
	listN    int
	deleted  []string
	created  []kafka.CreateTopicSpec
	cloned   []clonedPair
	altered  []alteredConfig
	alterErr error
	configs  map[string][]kafka.TopicConfig
	parts    map[string][]kafka.PartitionDetail
	cloneErr error
	// clonePartial, when non-nil, is the sequence of progress events to
	// emit in order from the channel returned by CloneTopic. Callers MUST
	// terminate it with a Done=true event; otherwise tests will hang on
	// the channel close path.
	clonePartial []kafka.CloneProgress

	// per-topic batch errors injected into BatchResult.Err.
	configErrs map[string]error
	sizeErrs   map[string]error
	wmErrs     map[string]error

	// denials emulates the Client-side dedup cache.
	denials map[kafka.Denial]struct{}
}

// fakeRefreshRepo is a stub [components.RefreshIntervalRepository] that
// records every save and serves a fixed load value.
type fakeRefreshRepo struct {
	loaded     time.Duration
	loadedOK   bool
	loadErr    error
	savedID    string
	savedValue time.Duration
	saveCalls  int
}

func (r *fakeRefreshRepo) LoadRefreshInterval(_ context.Context, _ string) (time.Duration, bool, error) {
	return r.loaded, r.loadedOK, r.loadErr
}

func (r *fakeRefreshRepo) SaveRefreshInterval(_ context.Context, screenID string, d time.Duration) error {
	r.savedID = screenID
	r.savedValue = d
	r.saveCalls++
	return nil
}

func TestNew_PersistedRefreshIntervalOverridesDefault(t *testing.T) {
	svc := newFakeService(nil, nil)
	repo := &fakeRefreshRepo{loaded: 42 * time.Second, loadedOK: true}

	m := topics.New(topics.Options{Service: svc, RefreshIntervals: repo})

	assert.Equal(t, 42*time.Second, m.RefreshInterval(),
		"persisted value must override the built-in default")
}

func TestNew_FallsBackToDefaultWhenRepoEmpty(t *testing.T) {
	svc := newFakeService(nil, nil)
	repo := &fakeRefreshRepo{loadedOK: false}

	m := topics.New(topics.Options{Service: svc, RefreshIntervals: repo})

	assert.Equal(t, 30*time.Second, m.RefreshInterval(),
		"no persisted row → screen mounts at the built-in default")
}

func TestOpenRefreshPicker_MountsOverlay(t *testing.T) {
	m := topics.New(topics.Options{Service: newFakeService(nil, nil)})

	m.OpenRefreshPicker()

	assert.True(t, m.HasOverlay(),
		"opening the picker must register as an overlay so the host yields esc to it")
	assert.Contains(t, m.View(), "Refresh interval",
		"View must render the picker overlay when one is mounted")
}

func TestPicker_DigitConfirmUpdatesIntervalAndPersists(t *testing.T) {
	repo := &fakeRefreshRepo{}
	m := topics.New(topics.Options{Service: newFakeService(nil, nil), RefreshIntervals: repo})
	require.Equal(t, 30*time.Second, m.RefreshInterval(), "precondition: default")
	m.OpenRefreshPicker()

	// digit 3 = 3rd preset = 5s (Manual / 1s / 5s / 10s / 30s / 1m / 5m) —
	// a value distinct from the default so we observe a real transition.
	_ = m.Update(keyPressRune('3'))

	assert.Equal(t, 5*time.Second, m.RefreshInterval(),
		"picker confirm must apply the picked preset to the live refresher")
	assert.False(t, m.HasOverlay(), "picker must clear itself after confirm")
	assert.Equal(t, 1, repo.saveCalls)
	assert.Equal(t, "topics", repo.savedID)
	assert.Equal(t, 5*time.Second, repo.savedValue)
}

func TestPicker_EscCancelsWithoutPersisting(t *testing.T) {
	repo := &fakeRefreshRepo{}
	m := topics.New(topics.Options{Service: newFakeService(nil, nil), RefreshIntervals: repo})
	require.Equal(t, 30*time.Second, m.RefreshInterval(), "precondition: default")
	m.OpenRefreshPicker()

	_ = m.Update(keyPress("esc"))

	assert.Equal(t, 30*time.Second, m.RefreshInterval(),
		"esc must leave the interval untouched")
	assert.False(t, m.HasOverlay())
	assert.Equal(t, 0, repo.saveCalls)
}

func newFakeService(topicsList []kafka.TopicSummary, listErr error) *fakeService {
	return &fakeService{
		topics:  append([]kafka.TopicSummary(nil), topicsList...),
		listErr: listErr,
		configs: map[string][]kafka.TopicConfig{},
		parts:   map[string][]kafka.PartitionDetail{},
	}
}

func (f *fakeService) ListCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listN
}

func (f *fakeService) Deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeService) Created() []kafka.CreateTopicSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.CreateTopicSpec(nil), f.created...)
}

func (f *fakeService) Cloned() []clonedPair {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clonedPair(nil), f.cloned...)
}

func (f *fakeService) ListTopics(_ context.Context) ([]kafka.TopicSummary, error) {
	f.mu.Lock()
	f.listN++
	out := append([]kafka.TopicSummary(nil), f.topics...)
	err := f.listErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) TopicWatermarks(_ context.Context, _ string) (kafka.TopicWatermarks, error) {
	return kafka.TopicWatermarks{}, nil
}

func (f *fakeService) TopicSize(_ context.Context, _ string) (int64, error) { return 0, nil }

func (f *fakeService) DescribeTopicConfigs(_ context.Context, topic string) ([]kafka.TopicConfig, error) {
	return f.configs[topic], nil
}

func (f *fakeService) DescribeAllTopicConfigs(_ context.Context, topic string) ([]kafka.TopicConfig, error) {
	return f.configs[topic], nil
}

func (f *fakeService) TopicWatermarksBatch(_ context.Context, names ...string) (map[string]kafka.BatchResult[kafka.TopicWatermarks], error) {
	out := make(map[string]kafka.BatchResult[kafka.TopicWatermarks], len(names))
	for _, t := range names {
		out[t] = kafka.BatchResult[kafka.TopicWatermarks]{Err: f.wmErrs[t]}
	}
	return out, nil
}

func (f *fakeService) TopicSizesBatch(_ context.Context, names ...string) (map[string]kafka.BatchResult[int64], error) {
	out := make(map[string]kafka.BatchResult[int64], len(names))
	for _, t := range names {
		out[t] = kafka.BatchResult[int64]{Err: f.sizeErrs[t]}
	}
	return out, nil
}

func (f *fakeService) DescribeTopicConfigsBatch(_ context.Context, names ...string) (map[string]kafka.BatchResult[[]kafka.TopicConfig], error) {
	out := make(map[string]kafka.BatchResult[[]kafka.TopicConfig], len(names))
	for _, t := range names {
		if err, ok := f.configErrs[t]; ok {
			out[t] = kafka.BatchResult[[]kafka.TopicConfig]{Err: err}
			continue
		}
		out[t] = kafka.BatchResult[[]kafka.TopicConfig]{Value: f.configs[t]}
	}
	return out, nil
}

func (f *fakeService) RegisterDenials(ds []kafka.Denial) []kafka.Denial {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.denials == nil {
		f.denials = make(map[kafka.Denial]struct{})
	}
	fresh := make([]kafka.Denial, 0, len(ds))
	for _, d := range ds {
		if _, seen := f.denials[d]; seen {
			continue
		}
		f.denials[d] = struct{}{}
		fresh = append(fresh, d)
	}
	return fresh
}

func (f *fakeService) TopicPartitions(_ context.Context, topic string) ([]kafka.PartitionDetail, error) {
	return f.parts[topic], nil
}

func (f *fakeService) CreateTopic(_ context.Context, spec kafka.CreateTopicSpec) error {
	f.mu.Lock()
	f.created = append(f.created, spec)
	f.topics = append(f.topics, kafka.TopicSummary{
		Name:       spec.Name,
		Partitions: int(spec.Partitions),
		Replicas:   int(spec.ReplicationFactor),
	})
	f.mu.Unlock()
	return nil
}

func (f *fakeService) DeleteTopic(_ context.Context, topic string) error {
	f.mu.Lock()
	f.deleted = append(f.deleted, topic)
	out := f.topics[:0]
	for _, t := range f.topics {
		if t.Name != topic {
			out = append(out, t)
		}
	}
	f.topics = out
	f.mu.Unlock()
	return nil
}

func (f *fakeService) AlterTopicConfig(_ context.Context, topic, key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.alterErr != nil {
		return f.alterErr
	}
	f.altered = append(f.altered, alteredConfig{topic: topic, key: key, value: value})
	cfgs := f.configs[topic]
	updated := make([]kafka.TopicConfig, 0, len(cfgs)+1)
	replaced := false
	for _, c := range cfgs {
		if c.Key == key {
			updated = append(updated, kafka.TopicConfig{Key: key, Value: value, Source: "DYNAMIC_TOPIC_CONFIG"})
			replaced = true
			continue
		}
		updated = append(updated, c)
	}
	if !replaced {
		updated = append(updated, kafka.TopicConfig{Key: key, Value: value, Source: "DYNAMIC_TOPIC_CONFIG"})
	}
	f.configs[topic] = updated
	return nil
}

func (f *fakeService) Altered() []alteredConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]alteredConfig(nil), f.altered...)
}

func (f *fakeService) CloneTopic(_ context.Context, src, dst string, _ kafka.CloneOptions) (<-chan kafka.CloneProgress, error) {
	f.mu.Lock()
	f.cloned = append(f.cloned, clonedPair{src: src, dst: dst})
	err := f.cloneErr
	partial := append([]kafka.CloneProgress(nil), f.clonePartial...)
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(partial) > 0 {
		ch := make(chan kafka.CloneProgress, len(partial))
		for _, p := range partial {
			ch <- p
		}
		close(ch)
		return ch, nil
	}
	ch := make(chan kafka.CloneProgress, 1)
	ch <- kafka.CloneProgress{Total: 0, Copied: 0, Done: true}
	close(ch)
	return ch, nil
}

func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "left":
		return tea.KeyPressMsg{Code: tea.KeyLeft}
	case "right":
		return tea.KeyPressMsg{Code: tea.KeyRight}
	case "ctrl+s":
		return tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl}
	case "ctrl+b":
		return tea.KeyPressMsg{Code: 'b', Mod: tea.ModCtrl}
	case "ctrl+d":
		return tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}
	case "ctrl+f":
		return tea.KeyPressMsg{Code: 'f', Mod: tea.ModCtrl}
	case "ctrl+u":
		return tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl}
	case "pgup":
		return tea.KeyPressMsg{Code: tea.KeyPgUp}
	case "pgdown":
		return tea.KeyPressMsg{Code: tea.KeyPgDown}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func keyPressRune(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}
