import { computed, defineComponent, onBeforeUnmount, onMounted, ref, watch } from "vue";

import {
  AppLink,
  AsyncState,
  ChartContainer,
  MetricCard,
} from "../components/index.js";
import { formatNumber, locale, t } from "../i18n.js";
import { preferences } from "../stores/preferences.js";
import { api } from "../utils/api.js";

const operationDefinitions = [
  {field: "puts", labelKey: "dashboard.puts"},
  {field: "reserves", labelKey: "common.reserves"},
  {field: "deletes", labelKey: "dashboard.deletes"},
  {field: "releases", labelKey: "dashboard.releases"},
  {field: "buries", labelKey: "dashboard.buries"},
  {field: "kicks", labelKey: "dashboard.kicks"},
  {field: "timeouts", labelKey: "dashboard.timeouts"},
  {field: "touches", labelKey: "dashboard.touches"},
];

const maxHistoryPoints = 120;

function percentage(value, total) {
  const result = total > 0 ? value / total * 100 : 0;
  return `${formatNumber(result, {maximumFractionDigits: 1})}%`;
}

function sampleLabel(timestamp) {
  return new Intl.DateTimeFormat(locale.value, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(timestamp));
}

export default defineComponent({
  name: "MetricsView",
  components: {AppLink, AsyncState, ChartContainer, MetricCard},
  setup() {
    const error = ref("");
    const history = ref([]);
    const loading = ref(true);
    const previousSample = ref(null);
    const snapshot = ref(null);
    let timer = 0;

    const overview = computed(() => snapshot.value?.overview || {});
    const topics = computed(() => snapshot.value?.topics || []);
    const currentRate = computed(() => history.value.at(-1)?.value || 0);
    const buriedRatio = computed(() => percentage(
      Number(overview.value.buried_jobs) || 0,
      Number(overview.value.total_jobs) || 0,
    ));
    const queueDepth = computed(() => [
      {name: t("state.ready"), value: Number(overview.value.ready_jobs) || 0},
      {name: t("state.delayed"), value: Number(overview.value.delayed_jobs) || 0},
      {name: t("state.reserved"), value: Number(overview.value.reserved_jobs) || 0},
      {name: t("state.buried"), value: Number(overview.value.buried_jobs) || 0},
    ]);
    const operationStatistics = computed(() => {
      const rows = operationDefinitions.map((definition) => ({
        ...definition,
        count: Number(overview.value[definition.field]) || 0,
      }));
      const total = rows.reduce((sum, row) => sum + row.count, 0);
      return rows.map((row) => ({...row, percentage: percentage(row.count, total)}));
    });

    function recordSample(data) {
      const timestamp = Date.parse(data.timestamp) || Date.now();
      const puts = Number(data.overview?.puts) || 0;
      let rate = 0;

      if (previousSample.value && timestamp > previousSample.value.timestamp) {
        const elapsedSeconds = (timestamp - previousSample.value.timestamp) / 1000;
        rate = Math.max(0, puts - previousSample.value.puts) / elapsedSeconds;
      } else {
        const startedAt = Date.parse(data.overview?.started_at);
        const elapsedSeconds = Number.isFinite(startedAt) ? (timestamp - startedAt) / 1000 : 0;
        rate = elapsedSeconds > 0 ? puts / elapsedSeconds : 0;
      }

      const nextPoint = {
        time: sampleLabel(timestamp),
        timestamp,
        value: Number(rate.toFixed(3)),
      };
      const lastPoint = history.value.at(-1);
      if (lastPoint?.timestamp === timestamp) {
        history.value = [...history.value.slice(0, -1), nextPoint];
      } else {
        history.value = [...history.value, nextPoint].slice(-maxHistoryPoints);
      }
      previousSample.value = {puts, timestamp};
    }

    async function load() {
      try {
        const data = await api("api/metrics");
        snapshot.value = data;
        recordSample(data);
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.loadMetrics");
      } finally {
        loading.value = false;
      }
    }

    function schedule() {
      window.clearInterval(timer);
      if (preferences.pollingInterval > 0) {
        timer = window.setInterval(load, preferences.pollingInterval);
      }
    }

    watch(() => preferences.pollingInterval, schedule);
    onMounted(() => {
      load();
      schedule();
    });
    onBeforeUnmount(() => window.clearInterval(timer));

    return {
      buriedRatio,
      currentRate,
      error,
      history,
      load,
      loading,
      operationStatistics,
      overview,
      queueDepth,
      topics,
    };
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('metrics.loading')" :error-title="$t('metrics.loadError')" @retry="load" />
    <template v-else>
      <div class="flex justify-between items-center h-16">
        <div class="flex items-center space-x-4"><AppLink class="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</AppLink><span class="text-gray-400 dark:text-[#6e7681]">/</span><span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('nav.metrics') }}</span></div>
        <button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refresh') }}</button>
      </div>
      <div class="mb-8"><h1 class="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('metrics.title') }}</h1><p class="text-gray-600 dark:text-[#8b949e]">{{ $t('metrics.description') }}</p></div>

      <section class="mb-8"><h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('metrics.kpi') }}</h2><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard :title="$t('metrics.enqueueRate')" :value="$t('metrics.perSecond', {count: $formatNumber(currentRate, {maximumFractionDigits: 3})})" icon="↗" color="green" />
        <MetricCard :title="$t('metrics.queueDepth')" :value="overview.total_jobs || 0" icon="≡" color="blue" />
        <MetricCard :title="$t('metrics.buriedRatio')" :value="buriedRatio" icon="!" color="red" />
        <MetricCard :title="$t('metrics.waitingWorkers')" :value="overview.total_waiting_workers || 0" icon="…" color="purple" />
      </div></section>

      <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <ChartContainer :title="$t('metrics.enqueueRateTrend')" type="line" :data="history" />
        <ChartContainer :title="$t('metrics.queueDepth')" type="bar" :data="queueDepth" />
      </section>

      <section class="rounded-lg shadow-sm p-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
        <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('metrics.detailedStatistics') }}</h2>
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div class="overflow-x-auto"><h3 class="text-lg font-medium mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('metrics.operationStatistics') }}</h3><table class="w-full"><thead><tr class="border-b border-gray-200 dark:border-[#30363d]"><th class="text-left py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">{{ $t('common.type') }}</th><th class="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">{{ $t('common.count') }}</th><th class="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">{{ $t('common.percentage') }}</th></tr></thead><tbody><tr v-for="row in operationStatistics" :key="row.field" class="border-b border-gray-200 dark:border-[#30363d]"><td class="py-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t(row.labelKey) }}</td><td class="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">{{ $formatNumber(row.count) }}</td><td class="text-right py-2 text-gray-600 dark:text-[#8b949e]">{{ row.percentage }}</td></tr></tbody></table></div>
          <div class="overflow-x-auto"><h3 class="text-lg font-medium mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('metrics.topicStatus') }}</h3><table class="w-full"><thead><tr class="border-b border-gray-200 dark:border-[#30363d]"><th class="text-left py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">{{ $t('common.topic') }}</th><th v-for="key in ['common.total','common.ready','common.delayed','common.reserved','common.buried']" :key="key" class="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">{{ $t(key) }}</th></tr></thead><tbody><tr v-for="topic in topics" :key="topic.name" class="border-b border-gray-200 dark:border-[#30363d]"><td class="py-2 text-gray-900 dark:text-[#c9d1d9]">{{ topic.name }}</td><td v-for="(value, index) in [topic.total_jobs,topic.ready_jobs,topic.delayed_jobs,topic.reserved_jobs,topic.buried_jobs]" :key="index" class="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">{{ $formatNumber(value) }}</td></tr><tr v-if="topics.length === 0"><td colspan="6" class="py-8 text-center text-gray-500 dark:text-[#8b949e]">{{ $t('topics.emptyTitle') }}</td></tr></tbody></table></div>
        </div>
      </section>
    </template>
  `,
});
