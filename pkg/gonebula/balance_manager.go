package gonebula

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	nmrand "github.com/kikimo/nebula-monkey/pkg/rand"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type BalanceManager struct {
	space   string
	session *nebula.Session
}

func (bm *BalanceManager) addAllZones() error {
	zones, err := GetZones(bm.session)
	if err != nil {
		return err
	}

	for _, z := range zones {
		ExecGQL(bm.session, fmt.Sprintf("alter space %s add zone %s", bm.space, z))
	}

	return nil
}

func (bm *BalanceManager) doBalanceAcrossZoneAndWait(ctx context.Context) (int64, error) {
	bm.addAllZones()
	time.Sleep(4 * time.Second)

	rs := ExeGqlNoError(bm.session, "submit job balance across zone")
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance job id: %d", *jobID)

	return *jobID, nil
	// return bm.waitForBalanceJob(ctx, *jobID)
}

func (bm *BalanceManager) BalanceAcrossZoneAndWait(ctx context.Context, retry bool) error {
	var jobID int64
	var err error
	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return fmt.Errorf("stopped")
		default:
		}

		jobID, err = bm.doBalanceAcrossZoneAndWait(ctx)
		if err != nil {
			return err
		}

		if !bm.IsEmptyJob(jobID) {
			break
		}

		glog.Infof("empty job id, retry")
	}

	for {
		status, err := bm.waitForBalanceJob(ctx, jobID)
		if err != nil {
			return err
		}

		if status == "FINISHED" {
			return nil
		}

		if bm.IsEmptyJob(jobID) {
			glog.Infof("empty job %d, return now", jobID)
			return nil
		}

		if !retry {
			return fmt.Errorf("balance across zone failed")
		}

		glog.Infof("job %d failed, retrying...", jobID)
		ExeGqlNoError(bm.session, fmt.Sprintf("recover job %d", jobID))
	}
}

func (bm *BalanceManager) doBalanceRemoveZoneAndWait(ctx context.Context, numZones int) (int64, error) {
	zones, err := GetZones(bm.session)
	if err != nil {
		return 0, err
	}
	glog.Infof("total zones: %+v", zones)

	if len(zones)/2 < numZones {
		return 0, fmt.Errorf("cannot remove %d zones, greater than half of total zone number %d", numZones, len(zones))
	}

	victimIndexes := nmrand.RandomChoiceFrom(len(zones), 2)
	// glog.Info("victim indexes: %+v", victimIndexes)
	victims := []string{}
	for _, idx := range victimIndexes {
		glog.V(1).Infof("adding victim: %s", zones[idx])
		victims = append(victims, zones[idx])
	}
	glog.V(1).Infof("victimes: %+v", victims)

	strings.Join(victims, ",")
	balanceRemoveStmt := fmt.Sprintf("submit job balance across zone remove %s", strings.Join(victims, ","))
	glog.Infof("balance removing zone: %+v", victims)
	rs := ExeGqlNoError(bm.session, balanceRemoveStmt)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance remove zone job id: %d", *jobID)

	return *jobID, nil
	// return bm.waitForBalanceJob(ctx, *jobID)
}

func (bm *BalanceManager) BalanceRemoveZonesAndWait(ctx context.Context, numZones int, retry bool) (chan struct{}, error) {
	jobID, err := bm.doBalanceRemoveZoneAndWait(ctx, numZones)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{}, 1)

	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return nil, fmt.Errorf("stopped")
		default:
		}

		status, err := bm.waitForBalanceJob(ctx, jobID)
		if err != nil {
			return nil, err
		}

		if status == "FINISHED" {
			return done, nil
		}

		if bm.IsEmptyJob(jobID) {
			glog.Infof("empty job %d, return now", jobID)
			return done, nil
		}

		if !retry {
			return nil, fmt.Errorf("balance across zone failed")
		}

		glog.Infof("job %d failed, retrying...", jobID)
		ExeGqlNoError(bm.session, fmt.Sprintf("recover job %d", jobID))
	}
}

func (bm *BalanceManager) doWaitForBalanceJob(ctx context.Context, jobID int64) chan string {
	waitDone := make(chan string, 1)
	localDone := make(chan string)
	var stopped int32 = 0

	go func() {
		for atomic.LoadInt32(&stopped) == 0 {
			rs := ExeGqlNoError(bm.session, "show jobs")
			found := false
			for _, row := range rs.GetRows() {
				if jobID != *row.Values[0].IVal {
					continue
				}

				found = true
				status := string(row.Values[2].SVal)
				if status == "FINISHED" || status == "FAILED" {
					localDone <- status
					return
				}

				glog.V(1).Infof("job %d status %s, waiting...", jobID, status)
				time.Sleep(4 * time.Second)
				break
			}

			if !found {
				localDone <- "error, job not found"
				return
			}
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			atomic.StoreInt32(&stopped, 1)
			waitDone <- "error, stopped"
		case ret := <-localDone:
			waitDone <- ret
		}
	}()

	return waitDone
}

func (bm *BalanceManager) waitForBalanceJob(ctx context.Context, jobID int64) (string, error) {
	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return "", fmt.Errorf("stopped")
		default:
		}

		rs := ExeGqlNoError(bm.session, "show jobs")
		found := false
		for _, row := range rs.GetRows() {
			if jobID != *row.Values[0].IVal {
				continue
			}

			found = true
			status := string(row.Values[2].SVal)
			if status == "FINISHED" || status == "FAILED" {
				return status, nil
			}

			glog.V(1).Infof("job %d status %s, waiting...", jobID, status)
			time.Sleep(4 * time.Second)
			break
		}

		if !found {
			return "", fmt.Errorf("job %d not found", jobID)
		}
	}
}

func (bm *BalanceManager) IsEmptyJob(jobID int64) bool {
	rs, err := ExecGQL(bm.session, fmt.Sprintf("show job %d", jobID))
	if err != nil {
		glog.Warningf("error testing job: %+v", err)
		return true
	}
	glog.Infof("row count for job %d: %d", jobID, len(rs.GetRows()))

	return len(rs.GetRows()) == 2
}

func NewBalanceManager(session *nebula.Session, space string) *BalanceManager {
	bm := &BalanceManager{
		session: session,
		space:   space,
	}

	return bm
}
