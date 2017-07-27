package stats

type CounterStat interface {
	Inc()
	Add(float64)
}

type GaugeStat interface {
	Set(float64)
	Get() float64

	Inc()
	Add(float64)

	Dec()
	Sub(float64)
}

type SummaryStat interface {
	Observe(float64)
}

type StatsFactory interface {
	NewCounter(
		metric string,
		tags map[string]string) CounterStat

	NewGauge(
		metric string,
		tags map[string]string) GaugeStat

	NewSummary(
		metric string,
		tags map[string]string) SummaryStat
}
