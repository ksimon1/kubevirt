/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2018 Red Hat, Inc.
 *
 */

package prometheus

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	libvirt "libvirt.org/libvirt-go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	k6tv1 "kubevirt.io/client-go/api/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
	"kubevirt.io/client-go/version"
	"kubevirt.io/kubevirt/pkg/util/lookup"
	cmdclient "kubevirt.io/kubevirt/pkg/virt-handler/cmd-client"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/stats"
)

const statsMaxAge time.Duration = collectionTimeout + 2*time.Second // "a bit more" than timeout, heuristic again

var (

	// Formatter used to sanitize k8s metadata into metric labels
	labelFormatter = strings.NewReplacer(".", "_", "/", "_", "-", "_")

	// Preffixes used when transforming K8s metadata into metric labels
	labelPrefix      = "kubernetes_vmi_label_"
	annotationPrefix = "vm.kubevirt.io/"

	// see https://www.robustperception.io/exposing-the-software-version-to-prometheus
	versionDesc = prometheus.NewDesc(
		"kubevirt_info",
		"Version information",
		[]string{"goversion", "kubeversion"},
		nil,
	)

	// higher-level, telemetry-friendly metrics
	vmiCountDesc = prometheus.NewDesc(
		"kubevirt_vmi_phase_count",
		"VMI phase.",
		[]string{
			"node", "phase", "os", "workload", "flavor",
		},
		nil,
	)
)

func tryToPushMetric(desc *prometheus.Desc, mv prometheus.Metric, err error, ch chan<- prometheus.Metric) {
	if err != nil {
		log.Log.V(4).Warningf("Error creating the new const metric for %s: %s", desc, err)
		return
	}
	ch <- mv
}

func (metrics *vmiMetrics) updateMemory(mem *stats.DomainStatsMemory) {
	if mem.RSSSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_resident_bytes",
			"resident set size of the process running the domain.",
			prometheus.GaugeValue,
			float64(mem.RSS)*1024,
		)
	}

	if mem.AvailableSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_available_bytes",
			"amount of usable memory as seen by the domain.",
			prometheus.GaugeValue,
			float64(mem.Available)*1024,
		)
	}

	if mem.UnusedSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_unused_bytes",
			"amount of unused memory as seen by the domain.",
			prometheus.GaugeValue,
			float64(mem.Unused)*1024,
		)
	}

	if mem.SwapInSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_swap_in_traffic_bytes_total",
			"Swap in memory traffic in bytes",
			prometheus.GaugeValue,
			float64(mem.SwapIn)*1024,
		)
	}

	if mem.SwapOutSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_swap_out_traffic_bytes_total",
			"Swap out memory traffic in bytes",
			prometheus.GaugeValue,
			float64(mem.SwapOut)*1024,
		)
	}

	if mem.MajorFaultSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_pgmajfault",
			"The number of page faults when disk IO was required.",
			prometheus.CounterValue,
			float64(mem.MajorFault),
		)
	}

	if mem.MinorFaultSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_pgminfault",
			"The number of other page faults, when disk IO was not required.",
			prometheus.CounterValue,
			float64(mem.MinorFault),
		)
	}

	if mem.ActualBalloonSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_actual_balloon_bytes",
			"current balloon bytes.",
			prometheus.GaugeValue,
			float64(mem.ActualBalloon)*1024,
		)
	}

	if mem.UsableSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_usable_bytes",
			"The amount of memory which can be reclaimed by balloon without causing host swapping in bytes.",
			prometheus.GaugeValue,
			float64(mem.Usable)*1024,
		)
	}

	if mem.TotalSet {
		metrics.pushCommonMetric(
			"kubevirt_vmi_memory_used_total_bytes",
			"The amount of memory in bytes used by the domain.",
			prometheus.GaugeValue,
			float64(mem.Total)*1024,
		)
	}
}

func (metrics *vmiMetrics) updateVcpu(vcpuStats []stats.DomainStatsVcpu) {
	for vcpuIdx, vcpu := range vcpuStats {
		stringVcpuIdx := fmt.Sprintf("%d", vcpuIdx)

		if vcpu.StateSet && vcpu.TimeSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_vcpu_seconds",
				"Vcpu elapsed time.",
				prometheus.CounterValue,
				float64(vcpu.Time/1000000000),
				[]string{"id", "state"},
				[]string{stringVcpuIdx, humanReadableState(vcpu.State)},
			)
		}

		if vcpu.WaitSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_vcpu_wait_seconds",
				"vcpu time spent by waiting on I/O.",
				prometheus.CounterValue,
				float64(vcpu.Wait/1000000),
				[]string{"id"},
				[]string{stringVcpuIdx},
			)
		}
	}
}

func (metrics *vmiMetrics) updateBlock(blkStats []stats.DomainStatsBlock) {
	for blockIdx, block := range blkStats {
		if !block.NameSet {
			log.Log.V(4).Warningf("Name not set for block device#%d", blockIdx)
			continue
		}

		if block.RdReqsSet || block.WrReqsSet {
			desc := metrics.newPrometheusDesc(
				"kubevirt_vmi_storage_iops_total",
				"I/O operation performed.",
				[]string{"drive", "type"},
			)

			if block.RdReqsSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.RdReqs), []string{block.Name, "read"})
			}
			if block.WrReqsSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.WrReqs), []string{block.Name, "write"})
			}
		}

		if block.RdBytesSet || block.WrBytesSet {
			desc := metrics.newPrometheusDesc(
				"kubevirt_vmi_storage_traffic_bytes_total",
				"storage traffic.",
				[]string{"drive", "type"},
			)

			if block.RdBytesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.RdBytes), []string{block.Name, "read"})
			}
			if block.WrBytesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.WrBytes), []string{block.Name, "write"})
			}
		}

		if block.RdTimesSet || block.WrTimesSet {
			desc := metrics.newPrometheusDesc(
				"kubevirt_vmi_storage_times_ms_total",
				"storage operation time.",
				[]string{"drive", "type"},
			)

			if block.RdTimesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.RdTimes), []string{block.Name, "read"})
			}
			if block.WrTimesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(block.WrTimes), []string{block.Name, "write"})
			}
		}
	}
}

func (metrics *vmiMetrics) updateNetwork(netStats []stats.DomainStatsNet) {
	for _, net := range netStats {
		if !net.NameSet {
			continue
		}

		ifaceLabel := net.Name
		if net.AliasSet {
			ifaceLabel = net.Alias
		}

		netLabels := []string{"interface"}
		netLabelValues := []string{ifaceLabel}

		if net.RxBytesSet || net.TxBytesSet {
			desc := metrics.newPrometheusDesc(
				"kubevirt_vmi_network_traffic_bytes_total",
				"network traffic.",
				[]string{"interface", "type"},
			)

			if net.RxBytesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(net.RxBytes), []string{net.Name, "rx"})
				metrics.pushCustomMetric(
					"kubevirt_vmi_network_receive_bytes_total",
					"Network traffic receive in bytes",
					prometheus.CounterValue,
					float64(net.RxBytes),
					netLabels,
					netLabelValues,
				)
			}

			if net.TxBytesSet {
				metrics.pushPrometheusMetric(desc, prometheus.CounterValue, float64(net.TxBytes), []string{net.Name, "tx"})
				metrics.pushCustomMetric(
					"kubevirt_vmi_network_transmit_bytes_total",
					"Network traffic transmit in bytes",
					prometheus.CounterValue,
					float64(net.TxBytes),
					netLabels,
					netLabelValues,
				)
			}
		}

		if net.RxPktsSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_receive_packets_total",
				"Network traffic receive packets",
				prometheus.CounterValue,
				float64(net.RxPkts),
				netLabels,
				netLabelValues,
			)
		}

		if net.TxPktsSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_transmit_packets_total",
				"Network traffic transmit packets",
				prometheus.CounterValue,
				float64(net.TxPkts),
				netLabels,
				netLabelValues,
			)
		}

		if net.RxErrsSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_receive_errors_total",
				"Network receive error packets",
				prometheus.CounterValue,
				float64(net.RxErrs),
				netLabels,
				netLabelValues,
			)
		}

		if net.TxErrsSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_transmit_errors_total",
				"Network transmit error packets",
				prometheus.CounterValue,
				float64(net.TxErrs),
				netLabels,
				netLabelValues,
			)
		}

		if net.RxDropSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_receive_packets_dropped_total",
				"The number of rx packets dropped on vNIC interfaces.",
				prometheus.CounterValue,
				float64(net.RxDrop),
				netLabels,
				netLabelValues,
			)
		}

		if net.TxDropSet {
			metrics.pushCustomMetric(
				"kubevirt_vmi_network_transmit_packets_dropped_total",
				"The number of tx packets dropped on vNIC interfaces.",
				prometheus.CounterValue,
				float64(net.TxDrop),
				netLabels,
				netLabelValues,
			)
		}
	}
}

type vmiCountMetric struct {
	Phase    string
	OS       string
	Workload string
	Flavor   string
}

func (vmc *vmiCountMetric) UpdateFromAnnotations(annotations map[string]string) {
	if val, ok := annotations[annotationPrefix+"os"]; ok {
		vmc.OS = val
	}

	if val, ok := annotations[annotationPrefix+"workload"]; ok {
		vmc.Workload = val
	}

	if val, ok := annotations[annotationPrefix+"flavor"]; ok {
		vmc.Flavor = val
	}
}

func newVMICountMetric(vmi *k6tv1.VirtualMachineInstance) vmiCountMetric {
	vmc := vmiCountMetric{
		Phase:    strings.ToLower(string(vmi.Status.Phase)),
		OS:       "<none>",
		Workload: "<none>",
		Flavor:   "<none>",
	}
	vmc.UpdateFromAnnotations(vmi.Annotations)
	return vmc
}

func makeVMICountMetricMap(vmis []*k6tv1.VirtualMachineInstance) map[vmiCountMetric]uint64 {
	countMap := make(map[vmiCountMetric]uint64)

	for _, vmi := range vmis {
		vmc := newVMICountMetric(vmi)
		countMap[vmc]++
	}
	return countMap
}

func updateVMIsPhase(nodeName string, vmis []*k6tv1.VirtualMachineInstance, ch chan<- prometheus.Metric) {
	countMap := makeVMICountMetricMap(vmis)

	for vmc, count := range countMap {
		mv, err := prometheus.NewConstMetric(
			vmiCountDesc, prometheus.GaugeValue,
			float64(count),
			nodeName, vmc.Phase, vmc.OS, vmc.Workload, vmc.Flavor,
		)
		if err != nil {
			continue
		}
		ch <- mv
	}
}

func updateVersion(ch chan<- prometheus.Metric) {
	verinfo := version.Get()
	ch <- prometheus.MustNewConstMetric(
		versionDesc, prometheus.GaugeValue,
		1.0,
		verinfo.GoVersion, verinfo.GitVersion,
	)
}

type Collector struct {
	virtCli       kubecli.KubevirtClient
	virtShareDir  string
	nodeName      string
	concCollector *concurrentCollector
}

func SetupCollector(virtCli kubecli.KubevirtClient, virtShareDir, nodeName string, MaxRequestsInFlight int) *Collector {
	log.Log.Infof("Starting collector: node name=%v", nodeName)
	co := &Collector{
		virtCli:       virtCli,
		virtShareDir:  virtShareDir,
		nodeName:      nodeName,
		concCollector: NewConcurrentCollector(MaxRequestsInFlight),
	}
	prometheus.MustRegister(co)
	return co
}

func (co *Collector) Describe(ch chan<- *prometheus.Desc) {
	// TODO: Use DescribeByCollect?
}

func newvmiSocketMapFromVMIs(baseDir string, vmis []*k6tv1.VirtualMachineInstance) vmiSocketMap {
	if len(vmis) == 0 {
		return nil
	}

	ret := make(vmiSocketMap)
	for _, vmi := range vmis {
		socketPath, err := cmdclient.FindSocketOnHost(vmi)
		if err != nil {
			// nothing to scrape...
			// this means there's no socket or the socket
			// is currently unreachable for this vmi.
			continue
		}
		ret[socketPath] = vmi
	}
	return ret
}

// Note that Collect could be called concurrently
func (co *Collector) Collect(ch chan<- prometheus.Metric) {
	updateVersion(ch)

	vmis, err := lookup.VirtualMachinesOnNode(co.virtCli, co.nodeName)
	if err != nil {
		log.Log.Reason(err).Errorf("failed to list all VMIs in '%s': %s", co.nodeName, err)
		return
	}

	if len(vmis) == 0 {
		log.Log.V(4).Infof("No VMIs detected")
		return
	}

	socketToVMIs := newvmiSocketMapFromVMIs(co.virtShareDir, vmis)
	scraper := &prometheusScraper{ch: ch}
	co.concCollector.Collect(socketToVMIs, scraper, collectionTimeout)

	updateVMIsPhase(co.nodeName, vmis, ch)
	return
}

type prometheusScraper struct {
	ch chan<- prometheus.Metric
}

type vmiStatsInfo struct {
	vmiSpec  *k6tv1.VirtualMachineInstance
	vmiStats *stats.DomainStats
}

func (ps *prometheusScraper) Scrape(socketFile string, vmi *k6tv1.VirtualMachineInstance) {
	ts := time.Now()
	cli, err := cmdclient.NewClient(socketFile)
	if err != nil {
		log.Log.Reason(err).Error("failed to connect to cmd client socket")
		// Ignore failure to connect to client.
		// These are all local connections via unix socket.
		// A failure to connect means there's nothing on the other
		// end listening.
		return
	}
	defer cli.Close()

	vmStats, exists, err := cli.GetDomainStats()
	if err != nil {
		log.Log.Reason(err).Errorf("failed to update stats from socket %s", socketFile)
		return
	}
	if !exists || vmStats.Name == "" {
		log.Log.V(2).Infof("disappearing VM on %s, ignored", socketFile) // VM may be shutting down
		return
	}

	// GetDomainStats() may hang for a long time.
	// If it wakes up past the timeout, there is no point in send back any metric.
	// In the best case the information is stale, in the worst case the information is stale *and*
	// the reporting channel is already closed, leading to a possible panic - see below
	elapsed := time.Now().Sub(ts)
	if elapsed > statsMaxAge {
		log.Log.Infof("took too long (%v) to collect stats from %s: ignored", elapsed, socketFile)
		return
	}

	ps.Report(socketFile, vmi, vmStats)
}

func (ps *prometheusScraper) Report(socketFile string, vmi *k6tv1.VirtualMachineInstance, vmStats *stats.DomainStats) {
	// statsMaxAge is an estimation - and there is not better way to do that. So it is possible that
	// GetDomainStats() takes enough time to lag behind, but not enough to trigger the statsMaxAge check.
	// In this case the next functions will end up writing on a closed channel. This will panic.
	// It is actually OK in this case to abort the goroutine that panicked -that's what we want anyway,
	// and the very reason we collect in throwaway goroutines. We need however to avoid dump stacktraces in the logs.
	// Since this is a known failure condition, let's handle it explicitly.
	defer func() {
		if err := recover(); err != nil {
			log.Log.V(2).Warningf("collector goroutine panicked for VM %s: %s", socketFile, err)
		}
	}()

	vmiMetrics := newVmiMetrics(vmi, ps.ch)
	vmiMetrics.updateMetrics(vmStats)

}

func Handler(MaxRequestsInFlight int) http.Handler {
	return promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				MaxRequestsInFlight: MaxRequestsInFlight,
			}),
	)
}

type vmiMetrics struct {
	k8sLabels      []string
	k8sLabelValues []string
	vmi            *k6tv1.VirtualMachineInstance
	ch             chan<- prometheus.Metric
}

func (metrics *vmiMetrics) updateMetrics(vmStats *stats.DomainStats) {
	metrics.updateKubernetesLabels()

	metrics.updateMemory(vmStats.Memory)
	metrics.updateVcpu(vmStats.Vcpu)
	metrics.updateBlock(vmStats.Block)
	metrics.updateNetwork(vmStats.Net)
}

func (metrics *vmiMetrics) newPrometheusDesc(name string, help string, customLabels []string) *prometheus.Desc {
	labels := []string{"node", "namespace", "name"} // Common labels
	labels = append(labels, customLabels...)
	labels = append(labels, metrics.k8sLabels...)
	return prometheus.NewDesc(name, help, labels, nil)
}

func (metrics *vmiMetrics) pushPrometheusMetric(desc *prometheus.Desc, valueType prometheus.ValueType, value float64, customLabelValues []string) {
	labelValues := []string{metrics.vmi.Status.NodeName, metrics.vmi.Namespace, metrics.vmi.Name}
	labelValues = append(labelValues, customLabelValues...)
	labelValues = append(labelValues, metrics.k8sLabelValues...)
	mv, err := prometheus.NewConstMetric(desc, valueType, value, labelValues...)
	tryToPushMetric(desc, mv, err, metrics.ch)
}

func (metrics *vmiMetrics) pushCommonMetric(name string, help string, valueType prometheus.ValueType, value float64) {
	metrics.pushCustomMetric(name, help, valueType, value, nil, nil)
}

func (metrics *vmiMetrics) pushCustomMetric(name string, help string, valueType prometheus.ValueType, value float64, customLabels []string, customLabelValues []string) {
	desc := metrics.newPrometheusDesc(name, help, customLabels)
	metrics.pushPrometheusMetric(desc, valueType, value, customLabelValues)
}

func (metrics *vmiMetrics) updateKubernetesLabels() {
	for label, val := range metrics.vmi.Labels {
		metrics.k8sLabels = append(metrics.k8sLabels, labelPrefix+labelFormatter.Replace(label))
		metrics.k8sLabelValues = append(metrics.k8sLabelValues, val)
	}
}

func newVmiMetrics(vmi *k6tv1.VirtualMachineInstance, ch chan<- prometheus.Metric) *vmiMetrics {
	return &vmiMetrics{
		vmi:            vmi,
		k8sLabels:      []string{},
		k8sLabelValues: []string{},
		ch:             ch,
	}
}

func humanReadableState(state int) string {
	switch state {
	case int(libvirt.VCPU_OFFLINE):
		return "offline"
	case int(libvirt.VCPU_BLOCKED):
		return "blocked"
	case int(libvirt.VCPU_RUNNING):
		return "running"
	default:
		return "unknown"
	}
}
