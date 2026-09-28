package returnsession

import "time"

// SetFenceSweepForTest shrinks the sweep's stranded-fence pass so a test
// can observe its batching, budget and rotation with a handful of rows.
func (s *Service) SetFenceSweepForTest(batch int, budget time.Duration) {
	s.fenceMu.Lock()
	defer s.fenceMu.Unlock()
	s.fenceBatch, s.fenceBudget = batch, budget
}
