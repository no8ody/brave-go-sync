package datastore

const (
	// Each period is roughly 3.5 days.
	periodDurationSecs  int64 = HistoryExpirationIntervalSecs / 4
	CurrentCountVersion int   = 2
)

// ClientItemCounts stores per-client object counts and history bucket counts.
type ClientItemCounts struct {
	ClientID                string
	ID                      string
	ItemCount               int
	HistoryItemCountPeriod1 int
	HistoryItemCountPeriod2 int
	HistoryItemCountPeriod3 int
	HistoryItemCountPeriod4 int
	LastPeriodChangeTime    int64
	Version                 int
}

// ClientItemCountByClientID implements sort.Interface for []ClientItemCounts
// based on ClientID.
type ClientItemCountByClientID []ClientItemCounts

func (a ClientItemCountByClientID) Len() int      { return len(a) }
func (a ClientItemCountByClientID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ClientItemCountByClientID) Less(i, j int) bool {
	return a[i].ClientID < a[j].ClientID
}

func (counts *ClientItemCounts) SumHistoryCounts() int {
	return counts.HistoryItemCountPeriod1 +
		counts.HistoryItemCountPeriod2 +
		counts.HistoryItemCountPeriod3 +
		counts.HistoryItemCountPeriod4
}
