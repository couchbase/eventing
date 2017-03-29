package servicemanager

func newCleanup(f func()) *cleanup {
	return &cleanup{
		canceled: false,
		f:        f,
	}
}

func (c *cleanup) run() {
	if !c.canceled {
		c.f()
		c.cancel()
	}
}

func (c *cleanup) cancel() {
	c.canceled = true
}
