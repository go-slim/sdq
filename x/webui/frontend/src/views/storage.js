import { computed, defineComponent, onMounted, ref } from "vue";

import { AppLink, AsyncState, StorageCard } from "../components/index.js";
import { formatNumber, t } from "../i18n.js";
import { api } from "../utils/api.js";
import { formatBytes, formatDate, formatDuration } from "../utils/format.js";

export default defineComponent({
  name: "StorageView",
  components: {AppLink, AsyncState, StorageCard},
  setup() {
    const error = ref("");
    const loading = ref(true);
    const storage = ref(null);

    const capacityStats = computed(() => storage.value ? [
      {label: t("storage.totalSize"), value: formatBytes(storage.value.total_size)},
      {label: t("storage.metadataSize"), value: formatBytes(storage.value.meta_size)},
      {label: t("storage.bodySize"), value: formatBytes(storage.value.body_size)},
    ] : []);
    const contentStats = computed(() => storage.value ? [
      {label: t("common.jobs"), value: formatNumber(storage.value.total_jobs)},
      {label: t("nav.topics"), value: formatNumber(storage.value.total_topics)},
      {label: t("storage.averageBodySize"), value: formatBytes(storage.value.avg_body_size)},
    ] : []);
    const runtimeStats = computed(() => storage.value ? [
      {label: t("storage.backend"), value: storage.value.name || t("common.unknown")},
      {label: t("dashboard.uptime"), value: formatDuration(storage.value.uptime)},
      {label: t("storage.startedAt"), value: formatDate(storage.value.started_at)},
    ] : []);
    const detailRows = computed(() => storage.value ? [
      {label: t("storage.averageMetadataSize"), value: formatBytes(storage.value.avg_meta_size)},
      {label: t("storage.averageBodySize"), value: formatBytes(storage.value.avg_body_size)},
      {label: t("storage.loadedMetadataSize"), value: formatBytes(storage.value.loaded_meta_size)},
      {label: t("storage.loadedBodySize"), value: formatBytes(storage.value.loaded_body_size)},
    ] : []);

    async function load() {
      loading.value = true;
      try {
        storage.value = await api("api/storage");
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.loadStorage");
      } finally {
        loading.value = false;
      }
    }

    onMounted(load);
    return {
      capacityStats,
      contentStats,
      detailRows,
      error,
      load,
      loading,
      runtimeStats,
      storage,
    };
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('storage.loading')" :error-title="$t('storage.loadError')" @retry="load" />
    <template v-else>
      <div class="max-w-7xl mx-auto"><div class="flex justify-between items-center h-16"><div class="flex items-center space-x-4"><AppLink class="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</AppLink><span class="text-gray-400 dark:text-[#6e7681]">/</span><span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('nav.storage') }}</span></div><button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refresh') }}</button></div></div>
      <div class="max-w-7xl mx-auto mb-8"><h1 class="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('storage.title') }}</h1><p class="text-gray-600 dark:text-[#8b949e]">{{ $t('storage.description') }}</p></div>

      <div v-if="storage" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-7xl mx-auto mb-8">
        <StorageCard :title="$t('storage.capacity')" :description="$t('storage.capacityDescription')" icon="💾" :stats="capacityStats" />
        <StorageCard :title="$t('storage.contents')" :description="$t('storage.contentsDescription')" icon="▤" :stats="contentStats" />
        <StorageCard :title="$t('storage.runtime')" :description="$t('storage.runtimeDescription')" icon="◷" :stats="runtimeStats" />
      </div>

      <section class="max-w-7xl mx-auto rounded-lg shadow-sm border p-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
        <h2 class="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('storage.loadedData') }}</h2>
        <p class="text-sm mb-4 text-gray-500 dark:text-[#8b949e]">{{ $t('storage.loadedDataDescription') }}</p>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div v-for="item in detailRows" :key="item.label" class="p-4 rounded bg-gray-50 dark:bg-[#0d1117]"><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ item.label }}</div><div class="mt-1 font-medium text-gray-900 dark:text-[#c9d1d9]">{{ item.value }}</div></div>
        </div>
      </section>
    </template>
  `,
});
