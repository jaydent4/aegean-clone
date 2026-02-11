package verifier

import (
	"log"
	"sync"
	"time"

	"aegean/common"
)

type Verifier struct {
	Name      string
	Verifiers []string
	Execs     []string
	// Local component channel
	ExecCh chan<- map[string]any
	// Hard-coded for now per user request.
	u int
	r int
	// Quorum sizes.
	execVerifyQuorum int
	phaseQuorum      int
	// Current verifier view.
	view int
	// State tracking per sequence number.
	slots map[int]*verifySlot
	// Committed token by sequence number.
	committed map[int]string
	mu        sync.Mutex
	// Out-of-order buffer for verify messages prioritized by view then seq.
	verifyBuffer *common.MultiOOOBuffer[map[string]any]
	// Timeout trigger for no-agreement path (paper mentions throughput-based trigger).
	viewChangeTimeout time.Duration
	// One timer per seq/view round.
	viewTimers map[int]*time.Timer
	// View-change certificate rounds keyed by target view.
	viewChangeRounds map[int]*viewChangeRound
}

type verifySlot struct {
	seqNum int
	view   int

	verifyVotes  map[string]map[string]struct{} // token -> exec_id set
	prepareVotes map[string]map[string]struct{} // token -> verifier_id set
	commitVotes  map[string]map[string]struct{} // token -> verifier_id set

	prepreparedToken string
	preparedToken    string
	committedToken   string
}

func NewVerifier(name string, verifiers []string, execs []string, execCh chan<- map[string]any) *Verifier {
	if execCh == nil {
		log.Fatalf("verifier component requires non-nil execCh")
	}
	v := &Verifier{
		Name:              name,
		Verifiers:         verifiers,
		Execs:             execs,
		ExecCh:            execCh,
		u:                 1,
		r:                 0,
		slots:             make(map[int]*verifySlot),
		committed:         make(map[int]string),
		verifyBuffer:      common.NewMultiOOOBuffer[map[string]any](),
		viewChangeTimeout: 2 * time.Second,
		viewTimers:        make(map[int]*time.Timer),
		viewChangeRounds:  make(map[int]*viewChangeRound),
	}
	v.execVerifyQuorum = common.MaxInt(v.u, v.r) + 1
	v.phaseQuorum = v.u + v.r + 1 // nV-u where nV=2u+r+1
	return v
}

func (v *Verifier) slotForLocked(seqNum int, view int) *verifySlot {
	slot, ok := v.slots[seqNum]
	if !ok || slot.view != view {
		slot = &verifySlot{
			seqNum:       seqNum,
			view:         view,
			verifyVotes:  make(map[string]map[string]struct{}),
			prepareVotes: make(map[string]map[string]struct{}),
			commitVotes:  make(map[string]map[string]struct{}),
		}
		v.slots[seqNum] = slot
	}
	return slot
}

func addVote(votes map[string]map[string]struct{}, token string, sender string) int {
	if _, ok := votes[token]; !ok {
		votes[token] = make(map[string]struct{})
	}
	votes[token][sender] = struct{}{}
	return len(votes[token])
}

func (v *Verifier) committedToken(seqNum int) (string, bool) {
	token, ok := v.committed[seqNum]
	return token, ok
}

func (v *Verifier) setCommitted(seqNum int, token string) {
	v.committed[seqNum] = token
}

func (v *Verifier) sendToVerifiers(msg map[string]any) {
	for _, verifierNode := range v.Verifiers {
		if verifierNode == v.Name {
			continue
		}
		if _, err := common.SendMessage(verifierNode, 8000, msg); err != nil {
			log.Printf("Failed to send to verifier %s: %v", verifierNode, err)
		}
	}
}

func (v *Verifier) sendVerifyResponse(seqNum int, view int, token string, forceSequential bool) {
	response := map[string]any{
		"type":             "verify_response",
		"view":             view,
		"seq_num":          seqNum,
		"token":            token,
		"force_sequential": forceSequential,
		"verifier_id":      v.Name,
	}

	for _, execNode := range v.Execs {
		if execNode == v.Name && v.ExecCh != nil {
			v.ExecCh <- response
			continue
		}
		if _, err := common.SendMessage(execNode, 8000, response); err != nil {
			log.Printf("Failed to send to exec %s: %v", execNode, err)
		}
	}
}

func (v *Verifier) maybeStartOrResetTimerLocked(seqNum int, view int) {
	timer, ok := v.viewTimers[seqNum]
	if ok && timer != nil {
		timer.Stop()
	}
	v.viewTimers[seqNum] = time.AfterFunc(v.viewChangeTimeout, func() {
		v.handleNoAgreement(seqNum, view)
	})
}

func (v *Verifier) stopTimerLocked(seqNum int) {
	timer, ok := v.viewTimers[seqNum]
	if ok && timer != nil {
		timer.Stop()
		delete(v.viewTimers, seqNum)
	}
}

func (v *Verifier) HandleVerifyMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	view := common.GetInt(payload, "view")
	v.verifyBuffer.Add(view, seqNum, payload)
	progressed := false
	for {
		if !v.flushNextVerify() {
			break
		}
		progressed = true
	}
	if progressed {
		return map[string]any{"status": "processed_or_buffered", "view": view, "seq_num": seqNum}
	}
	return map[string]any{"status": "buffered", "view": view, "seq_num": seqNum}
}

func (v *Verifier) HandlePrepareMessage(payload map[string]any) map[string]any {
	return v.applyPrepareMessage(payload)
}

func (v *Verifier) HandleCommitMessage(payload map[string]any) map[string]any {
	return v.applyCommitMessage(payload)
}

func (v *Verifier) HandleViewChangeMessage(payload map[string]any) map[string]any {
	return v.applyViewChangeMessage(payload)
}

func (v *Verifier) HandleNewViewMessage(payload map[string]any) map[string]any {
	return v.applyNewViewMessage(payload)
}

func parseView(payload map[string]any) (int, bool) {
	view := common.GetInt(payload, "view")
	if payload["view"] == nil {
		return 0, false
	}
	return view, true
}

func (v *Verifier) isKnownExec(execID string) bool {
	for _, exec := range v.Execs {
		if exec == execID {
			return true
		}
	}
	return false
}

func verifyVoteStats(votes map[string]map[string]struct{}) (int, int) {
	maxVotes := 0
	senders := make(map[string]struct{})
	for _, tokenVotes := range votes {
		if len(tokenVotes) > maxVotes {
			maxVotes = len(tokenVotes)
		}
		for sender := range tokenVotes {
			senders[sender] = struct{}{}
		}
	}
	return maxVotes, len(senders)
}
