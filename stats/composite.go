package stats

type compositeCounter struct {
	metrics []CounterStat
}

func (s compositeCounter) Inc() {
	for _, metric := range s.metrics {
		metric.Inc()
	}
}

func (s compositeCounter) Add(value float64) {
	for _, metric := range s.metrics {
		metric.Add(value)
	}
}

type compositeGauge struct {
	metrics []GaugeStat
}

func (s compositeGauge) Inc() {
	for _, metric := range s.metrics {
		metric.Inc()
	}
}

func (s compositeGauge) Add(value float64) {
	for _, metric := range s.metrics {
		metric.Add(value)
	}
}

func (s compositeGauge) Dec() {
	for _, metric := range s.metrics {
		metric.Dec()
	}
}

func (s compositeGauge) Sub(value float64) {
	for _, metric := range s.metrics {
		metric.Sub(value)
	}
}

func (s compositeGauge) Set(value float64) {
	for _, metric := range s.metrics {
		metric.Set(value)
	}
}

func (s compositeGauge) Get() float64 {
	if len(s.metrics) > 0 {
		// Assume that value is the same for all metrics.
		return s.metrics[0].Get()
	}
	return 0
}

type compositeSummary struct {
	metrics []SummaryStat
}

func (s compositeSummary) Observe(value float64) {
	for _, metric := range s.metrics {
		metric.Observe(value)
	}
}

type compositeStatsFactory struct {
	factories []StatsFactory
}

func NewCompositeFactory(factories ...StatsFactory) StatsFactory {
	return compositeStatsFactory{factories}
}

func (f compositeStatsFactory) NewCounter(
	metric string, tags map[string]string) CounterStat {

	metrics := make([]CounterStat, len(f.factories))
	for i, factory := range f.factories {
		metrics[i] = factory.NewCounter(metric, tags)
	}

	return compositeCounter{metrics}
}

func (f compositeStatsFactory) NewGauge(
	metric string, tags map[string]string) GaugeStat {

	metrics := make([]GaugeStat, len(f.factories))
	for i, factory := range f.factories {
		metrics[i] = factory.NewGauge(metric, tags)
	}

	return compositeGauge{metrics}
}

func (f compositeStatsFactory) NewSummary(
	metric string, tags map[string]string) SummaryStat {

	metrics := make([]SummaryStat, len(f.factories))
	for i, factory := range f.factories {
		metrics[i] = factory.NewSummary(metric, tags)
	}

	return compositeSummary{metrics}
}
