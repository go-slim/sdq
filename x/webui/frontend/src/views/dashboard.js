import { defineComponent, onBeforeUnmount, onMounted, ref, watch } from "vue";

import { AppLink, AsyncState, StatCard } from "../components/index.js";
import { t } from "../i18n.js";
import { api } from "../utils/api.js";
import { formatUptime } from "../utils/format.js";

export default defineComponent({
  name: "DashboardView",
  components: {AppLink, AsyncState, StatCard},
  setup() {
    const autoRefresh = ref(false);
    const error = ref("");
    const loading = ref(true);
    const overview = ref(null);
    const refreshInterval = ref(5000);
    const topics = ref([]);
    let timer = 0;

    async function load() {
      try {
        const [overviewData, topicsData] = await Promise.all([
          api("api/overview"),
          api("api/topics"),
        ]);
        overview.value = overviewData;
        topics.value = topicsData || [];
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.fetchData");
      } finally {
        loading.value = false;
      }
    }

    async function kickTopic(topic) {
      if (!window.confirm(t("confirm.kickTopic", {name: topic.name}))) return;
      try {
        await api(`api/topics/${encodeURIComponent(topic.name)}/kick`, {method: "POST"});
        await load();
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    function schedule() {
      window.clearInterval(timer);
      if (autoRefresh.value) timer = window.setInterval(load, refreshInterval.value);
    }

    watch([autoRefresh, refreshInterval], schedule);
    onMounted(load);
    onBeforeUnmount(() => window.clearInterval(timer));

    return {autoRefresh, error, formatUptime, kickTopic, load, loading, overview, refreshInterval, topics};
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('dashboard.loading')" tall @retry="load" />
    <template v-else>
      <div class="max-w-7xl mx-auto mb-8">
        <div class="flex justify-between items-center h-16">
          <div><h1 class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ $t('app.inspector') }}</h1><p class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('app.subtitle') }}</p></div>
          <div class="flex items-center space-x-4">
            <label class="flex items-center"><input v-model="autoRefresh" type="checkbox" class="mr-2"><span class="text-sm text-gray-900 dark:text-[#c9d1d9]">{{ $t('dashboard.autoRefresh') }}</span></label>
            <select v-if="autoRefresh" v-model.number="refreshInterval" class="border rounded px-2 py-1 text-sm bg-white dark:bg-[#0d1117] border-gray-300 dark:border-[#30363d] text-gray-900 dark:text-[#c9d1d9]">
              <option :value="1000">1{{ $t('unit.secondShort') }}</option><option :value="5000">5{{ $t('unit.secondShort') }}</option><option :value="10000">10{{ $t('unit.secondShort') }}</option><option :value="30000">30{{ $t('unit.secondShort') }}</option>
            </select>
            <button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refreshNow') }}</button>
          </div>
        </div>
      </div>

      <section class="max-w-7xl mx-auto mb-8">
        <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('dashboard.queueOverview') }}</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard :label="$t('dashboard.totalJobs')" :value="overview?.total_jobs || 0" color="blue" />
          <StatCard :label="$t('dashboard.readyJobs')" :value="overview?.ready_jobs || 0" color="green" />
          <StatCard :label="$t('dashboard.reservedJobs')" :value="overview?.reserved_jobs || 0" color="yellow" />
          <StatCard :label="$t('dashboard.delayedJobs')" :value="overview?.delayed_jobs || 0" color="yellow" />
          <StatCard :label="$t('dashboard.buriedJobs')" :value="overview?.buried_jobs || 0" color="red" />
          <StatCard :label="$t('nav.topics')" :value="overview?.total_topics || 0" color="gray" />
          <StatCard :label="$t('dashboard.waitingWorkers')" :value="overview?.total_waiting_workers || 0" color="blue" />
          <StatCard :label="$t('dashboard.uptime')" :value="formatUptime(overview?.uptime)" color="gray" />
        </div>
      </section>

      <section class="max-w-7xl mx-auto mb-8">
        <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('dashboard.operationStatistics') }}</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard :label="$t('dashboard.puts')" :value="overview?.puts || 0" color="green" /><StatCard :label="$t('common.reserves')" :value="overview?.reserves || 0" color="blue" />
          <StatCard :label="$t('dashboard.deletes')" :value="overview?.deletes || 0" color="red" /><StatCard :label="$t('dashboard.releases')" :value="overview?.releases || 0" color="yellow" />
          <StatCard :label="$t('dashboard.buries')" :value="overview?.buries || 0" color="red" /><StatCard :label="$t('dashboard.kicks')" :value="overview?.kicks || 0" color="blue" />
          <StatCard :label="$t('dashboard.timeouts')" :value="overview?.timeouts || 0" color="yellow" /><StatCard :label="$t('dashboard.touches')" :value="overview?.touches || 0" color="gray" />
        </div>
      </section>

      <section class="max-w-7xl mx-auto">
        <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('nav.topics') }}</h2>
        <div class="rounded-lg shadow overflow-x-auto bg-white dark:bg-[#161b22]">
          <table class="min-w-full">
            <thead class="bg-gray-50 dark:bg-[#0d1117]"><tr>
              <th class="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">{{ $t('dashboard.topicName') }}</th>
              <th v-for="key in ['common.total','common.ready','common.reserved','common.delayed','common.buried','common.actions']" :key="key" class="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">{{ $t(key) }}</th>
            </tr></thead>
            <tbody class="bg-white dark:bg-[#161b22] divide-y divide-gray-200 dark:divide-[#30363d]">
              <tr v-for="topic in topics" :key="topic.name" class="border-b border-gray-200 dark:border-[#30363d] hover:bg-gray-50 dark:hover:bg-[#21262d]">
                <td class="px-6 py-4"><AppLink :to="'topics/' + encodeURIComponent(topic.name)" class="accent-text font-medium">{{ topic.name }}</AppLink></td>
                <td v-for="(value, index) in [topic.total_jobs,topic.ready_jobs,topic.reserved_jobs,topic.delayed_jobs,topic.buried_jobs]" :key="index" class="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{{ $formatNumber(value) }}</td>
                <td class="px-6 py-4 text-center"><button v-if="topic.buried_jobs > 0" type="button" class="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600" @click="kickTopic(topic)">{{ $t('dashboard.kickAll') }}</button></td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </template>
  `,
});
