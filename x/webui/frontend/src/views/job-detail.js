import { computed, defineComponent, onMounted, ref, watch } from "vue";

import { AppLink, AsyncState, StateBadge } from "../components/index.js";
import { t } from "../i18n.js";
import { navigate } from "../router.js";
import { api } from "../utils/api.js";
import { formatDate, formatDuration } from "../utils/format.js";

const statistics = [
  {key: "job.reserves", name: "reserves"},
  {key: "job.timeouts", name: "timeouts"},
  {key: "job.releases", name: "releases"},
  {key: "job.buries", name: "buries"},
  {key: "job.kicks", name: "kicks"},
  {key: "job.touches", name: "touches"},
];

export default defineComponent({
  name: "JobDetailView",
  components: {AppLink, AsyncState, StateBadge},
  props: {
    id: {type: String, required: true},
  },
  setup(props) {
    const bodyError = ref("");
    const bodyInfo = ref(null);
    const error = ref("");
    const job = ref(null);
    const loading = ref(true);
    const body = computed(() => {
      if (!bodyInfo.value) return "";
      if (bodyInfo.value.encoding !== "utf-8") return bodyInfo.value.body;
      try {
        return JSON.stringify(JSON.parse(bodyInfo.value.body), null, 2);
      } catch {
        return bodyInfo.value.body;
      }
    });
    const bodySize = computed(() => bodyInfo.value?.size ?? job.value?.body_size ?? 0);

    async function load() {
      loading.value = true;
      try {
        job.value = await api(`api/jobs/${encodeURIComponent(props.id)}`);
        try {
          bodyInfo.value = await api(`api/jobs/${encodeURIComponent(props.id)}/body`);
          bodyError.value = "";
        } catch (cause) {
          bodyInfo.value = null;
          bodyError.value = cause.message || t("job.bodyUnavailable");
        }
        error.value = "";
      } catch (cause) {
        job.value = null;
        bodyInfo.value = null;
        bodyError.value = "";
        error.value = cause.message || t("error.jobNotFound", {id: props.id});
      } finally {
        loading.value = false;
      }
    }

    async function kick() {
      if (!window.confirm(t("confirm.kickJob", {id: props.id}))) return;
      try {
        await api(`api/jobs/${encodeURIComponent(props.id)}/kick`, {method: "POST"});
        window.alert(t("success.jobKicked"));
        await load();
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    async function remove() {
      if (!window.confirm(t("confirm.deleteJob", {id: props.id}))) return;
      try {
        await api(`api/jobs/${encodeURIComponent(props.id)}`, {method: "DELETE"});
        window.alert(t("success.jobDeleted"));
        navigate("");
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    function duration(value, empty = t("common.none")) {
      if (value === undefined || value === null || value === "" || value === 0 || value === "0s") return empty;
      return formatDuration(value);
    }

    onMounted(load);
    watch(() => props.id, load);

    return {body, bodyError, bodyInfo, bodySize, duration, error, formatDate, job, kick, load, loading, remove, statistics};
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('job.loading')" :error-title="$t('job.loadError')" tall @retry="load">
      <AppLink class="accent-solid ml-2 px-4 py-2 rounded">{{ $t('common.backToDashboard') }}</AppLink>
    </AsyncState>
    <AsyncState v-else-if="!job" empty :empty-title="$t('job.notFound')" :empty-description="$t('job.notFoundDescription', {id})" tall />
    <template v-else>
      <div class="max-w-7xl mx-auto mb-8">
        <div class="flex justify-between items-center h-16">
          <div class="flex items-center space-x-4">
            <AppLink class="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</AppLink>
            <span class="text-gray-400 dark:text-[#6e7681]">/</span>
            <AppLink :to="'topics/' + encodeURIComponent(job.topic)" class="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">{{ job.topic }}</AppLink>
            <span class="text-gray-400 dark:text-[#6e7681]">/</span>
            <span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('job.title', {id: job.id}) }}</span>
          </div>
          <button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refresh') }}</button>
        </div>
      </div>

      <section class="max-w-7xl mx-auto mb-8">
        <div class="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <div class="flex justify-between items-start mb-6">
            <div class="flex items-center space-x-4"><h2 class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ $t('job.title', {id: job.id}) }}</h2><StateBadge :state="job.state" /></div>
            <div class="flex space-x-2">
              <button v-if="job.state === 'buried'" type="button" class="bg-yellow-500 text-white px-4 py-2 rounded hover:bg-yellow-600" @click="kick">{{ $t('job.kick') }}</button>
              <button type="button" class="bg-red-600 text-white px-4 py-2 rounded hover:bg-red-700" @click="remove">{{ $t('job.delete') }}</button>
            </div>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('common.id') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">#{{ job.id }}</span></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('common.topic') }}</span><AppLink :to="'topics/' + encodeURIComponent(job.topic)" class="accent-text">{{ job.topic }}</AppLink></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('common.state') }}</span><StateBadge :state="job.state" /></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('common.priority') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">{{ job.priority }}</span></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('job.delay') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">{{ duration(job.delay) }}</span></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('job.ttr') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">{{ duration(job.ttr, '-') }}</span></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('common.createdAt') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">{{ formatDate(job.created_at) }}</span></div>
            <div class="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]"><span class="font-medium text-gray-700 dark:text-[#8b949e]">{{ $t('job.age') }}</span><span class="text-gray-900 dark:text-[#c9d1d9]">{{ job.age || $t('common.unknown') }}</span></div>
          </div>
        </div>
      </section>

      <section class="max-w-7xl mx-auto mb-8">
        <div class="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <h2 class="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{{ $t('job.statistics') }}</h2>
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div v-for="stat in statistics" :key="stat.name" class="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ $formatNumber(job[stat.name]) }}</div>
              <div class="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">{{ $t(stat.key) }}</div>
            </div>
          </div>
        </div>
      </section>

      <section class="max-w-7xl mx-auto">
        <div class="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <div class="flex justify-between items-center mb-4"><div class="flex items-center gap-2"><h2 class="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('job.body') }}</h2><span v-if="bodyInfo?.encoding === 'base64'" class="px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-600 dark:bg-[#21262d] dark:text-[#8b949e]">BASE64</span></div><span class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('job.bytes', {count: $formatNumber(bodySize)}) }}</span></div>
          <div v-if="bodyError" class="p-4 rounded text-sm bg-gray-50 text-gray-500 dark:bg-[#0d1117] dark:text-[#8b949e]">{{ $t('job.bodyUnavailable') }}: {{ bodyError }}</div>
          <pre v-else class="p-4 rounded overflow-x-auto text-sm bg-gray-50 text-gray-900 dark:bg-[#0d1117] dark:text-[#c9d1d9]">{{ body }}</pre>
        </div>
      </section>
    </template>
  `,
});
