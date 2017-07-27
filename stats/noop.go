package stats

var (
	NoOpStatsFactory StatsFactory
)

type noopCounter struct {
}

func (s noopCounter) Inc() {
}

func (s noopCounter) Add(v float64) {
}

type noopGauge struct {
}

func (s noopGauge) Inc() {
}

func (s noopGauge) Add(v float64) {
}

func (s noopGauge) Dec() {
}

func (s noopGauge) Sub(v float64) {
}

func (s noopGauge) Set(v float64) {
}

func (s noopGauge) Get() float64 {
	return 0
}

type noopSummary struct {
}

func (s noopSummary) Observe(v float64) {
}

type noopStatsFactory struct {
}

func (f noopStatsFactory) NewCounter(
	metric string,
	tags map[string]string) CounterStat {

	return noopCounter{}
}

func (f noopStatsFactory) NewGauge(
	metric string,
	tags map[string]string) GaugeStat {

	return noopGauge{}
}

func (f noopStatsFactory) NewSummary(
	metric string,
	tags map[string]string) SummaryStat {

	return noopSummary{}
}

func init() {
	NoOpStatsFactory = noopStatsFactory{}
}
