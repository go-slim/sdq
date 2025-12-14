package sdq

import "testing"

func TestStats_Record(t *testing.T) {
	stats := &Stats{}

	stats.recordPut()
	if stats.Puts != 1 {
		t.Errorf("Puts = %d, want 1", stats.Puts)
	}

	stats.recordReserve()
	if stats.Reserves != 1 {
		t.Errorf("Reserves = %d, want 1", stats.Reserves)
	}

	stats.recordDelete()
	if stats.Deletes != 1 {
		t.Errorf("Deletes = %d, want 1", stats.Deletes)
	}

	stats.recordRelease()
	if stats.Releases != 1 {
		t.Errorf("Releases = %d, want 1", stats.Releases)
	}

	stats.recordBury()
	if stats.Buries != 1 {
		t.Errorf("Buries = %d, want 1", stats.Buries)
	}

	stats.recordKick(1)
	if stats.Kicks != 1 {
		t.Errorf("Kicks = %d, want 1", stats.Kicks)
	}

	stats.recordKick(5)
	if stats.Kicks != 6 {
		t.Errorf("Kicks = %d, want 6", stats.Kicks)
	}

	stats.recordTimeout()
	if stats.Timeouts != 1 {
		t.Errorf("Timeouts = %d, want 1", stats.Timeouts)
	}

	stats.recordTouch()
	if stats.Touches != 1 {
		t.Errorf("Touches = %d, want 1", stats.Touches)
	}
}
