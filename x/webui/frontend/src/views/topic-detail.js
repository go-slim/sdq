import { defineComponent, onMounted, ref, watch } from "vue";

import {
  AppLink,
  AsyncState,
  JobRow,
  Pagination,
} from "../components/index.js";
import { t } from "../i18n.js";
import { api } from "../utils/api.js";

const states = ["", "ready", "delayed", "reserved", "buried"];

export default defineComponent({
  name: "TopicDetailView",
  components: {AppLink, AsyncState, JobRow, Pagination},
  props: {
    topic: {type: String, required: true},
  },
  setup(props) {
    const currentPage = ref(1);
    const currentState = ref("");
    const error = ref("");
    const jobsData = ref(null);
    const loading = ref(true);
    const topicData = ref(null);

    async function loadTopic() {
      topicData.value = await api(`api/topics/${encodeURIComponent(props.topic)}`);
    }

    async function loadJobs() {
      const params = new URLSearchParams({
        page: String(currentPage.value),
        page_size: "20",
      });
      if (currentState.value) params.set("state", currentState.value);
      jobsData.value = await api(
        `api/topics/${encodeURIComponent(props.topic)}/jobs?${params}`,
      );
    }

    async function load() {
      loading.value = true;
      try {
        await Promise.all([loadTopic(), loadJobs()]);
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.fetchTopic");
      } finally {
        loading.value = false;
      }
    }

    async function reloadJobs() {
      try {
        await loadJobs();
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.fetchJobs");
      }
    }

    async function kick(job) {
      if (!window.confirm(t("confirm.kickJob", {id: job.id}))) return;
      try {
        await api(`api/jobs/${job.id}/kick`, {method: "POST"});
        await load();
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    async function remove(job) {
      if (!window.confirm(t("confirm.deleteJob", {id: job.id}))) return;
      try {
        await api(`api/jobs/${job.id}`, {method: "DELETE"});
        await load();
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    function selectState(state) {
      currentState.value = state;
      currentPage.value = 1;
    }

    watch([currentState, currentPage], reloadJobs);
    watch(
      () => props.topic,
      () => {
        currentState.value = "";
        currentPage.value = 1;
        load();
      },
    );
    onMounted(load);

    return {
      currentPage,
      currentState,
      error,
      jobsData,
      kick,
      load,
      loading,
      Math,
      remove,
      selectState,
      states,
      topicData,
    };
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('topic.loading')" tall @retry="load" />
    <AsyncState v-else-if="!topicData" empty :empty-title="$t('topic.notFound')" :empty-description="$t('topic.notFoundDescription', {name: topic})" tall />
    <template v-else>
      <div class="max-w-7xl mx-auto mb-8">
        <div class="flex justify-between items-center h-16">
          <div class="flex items-center space-x-4">
            <AppLink class="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</AppLink>
            <span class="text-gray-400 dark:text-[#6e7681]">/</span>
            <span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ topic }}</span>
          </div>
          <button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refresh') }}</button>
        </div>
      </div>

      <section class="max-w-7xl mx-auto mb-8">
        <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('topic.statistics') }}</h2>
        <div class="grid grid-cols-1 md:grid-cols-5 gap-6">
          <div v-for="stat in [
            {label: $t('dashboard.totalJobs'), value: topicData.total_jobs, color: 'text-gray-900 dark:text-[#c9d1d9]'},
            {label: $t('common.ready'), value: topicData.ready_jobs, color: 'text-green-600 dark:text-[#3fb950]'},
            {label: $t('common.reserved'), value: topicData.reserved_jobs, color: 'text-blue-600 dark:text-[#58a6ff]'},
            {label: $t('common.delayed'), value: topicData.delayed_jobs, color: 'text-yellow-600 dark:text-[#d29922]'},
            {label: $t('common.buried'), value: topicData.buried_jobs, color: 'text-red-600 dark:text-[#f85149]'},
          ]" :key="stat.label" class="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div :class="['text-2xl font-bold', stat.color]">{{ $formatNumber(stat.value) }}</div>
            <div class="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">{{ stat.label }}</div>
          </div>
        </div>
      </section>

      <section class="max-w-7xl mx-auto">
        <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-4">
          <h2 class="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('topic.jobsTotal', {count: $formatNumber(jobsData?.total || 0)}) }}</h2>
          <div class="flex flex-wrap gap-2">
            <button
              v-for="state in states"
              :key="state || 'all'"
              type="button"
              :class="['px-4 py-2 rounded text-sm font-medium', currentState === state ? 'accent-solid' : 'bg-gray-200 text-gray-700 hover:bg-gray-300 dark:bg-[#21262d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]']"
              @click="selectState(state)"
            >{{ state ? $t('state.' + state) : $t('common.all') }}</button>
          </div>
        </div>

        <template v-if="jobsData?.jobs?.length">
          <div class="rounded-lg shadow overflow-x-auto bg-white dark:bg-[#161b22]">
            <table class="min-w-full">
              <thead class="bg-gray-50 dark:bg-[#0d1117]"><tr>
                <th class="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.id') }}</th>
                <th class="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.state') }}</th>
                <th class="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.priority') }}</th>
                <th class="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.reserves') }}</th>
                <th class="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.createdAt') }}</th>
                <th class="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">{{ $t('common.actions') }}</th>
              </tr></thead>
              <tbody class="bg-white divide-y divide-gray-200 dark:bg-[#161b22] dark:divide-[#30363d]">
                <JobRow v-for="job in jobsData.jobs" :key="job.id" :job="job" @kick="kick" @delete="remove" />
              </tbody>
            </table>
          </div>
          <Pagination :page="currentPage" :total-pages="jobsData.total_pages || 1" @change="currentPage = $event" />
          <div class="flex justify-center items-center mt-4 text-sm text-gray-600 dark:text-[#8b949e]">
            {{ $t('topic.showing', {from: $formatNumber(((currentPage - 1) * (jobsData.page_size || 20)) + 1), to: $formatNumber(Math.min(currentPage * (jobsData.page_size || 20), jobsData.total)), total: $formatNumber(jobsData.total)}) }}
          </div>
        </template>
        <div v-else class="rounded-lg p-12 text-center bg-white text-gray-500 dark:bg-[#161b22] dark:text-[#8b949e]">
          <p class="text-lg font-medium">{{ $t('topic.noJobs') }}</p><p class="mt-1">{{ $t('topic.noJobsDescription') }}</p>
          <button v-if="currentState" type="button" class="accent-solid mt-4 px-4 py-2 rounded" @click="selectState('')">{{ $t('topic.showAllJobs') }}</button>
        </div>
      </section>
    </template>
  `,
});
