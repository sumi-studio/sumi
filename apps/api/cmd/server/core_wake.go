package main

// startCoreWaker runs the Cloud core host's wake sweep (SUMI_CORE_WAKE_URL):
// personas with queued work and no live writer are woken remotely. Absent in
// a Local placement, where the Node host polls the state service itself.
func (a *application) startCoreWaker() {
	if a.coreWaker == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		a.coreWaker.Run(a.backgroundCtx)
	}()
}
