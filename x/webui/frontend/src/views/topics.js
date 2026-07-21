import { defineComponent, onMounted, ref } from "vue";

import { AsyncState, TopicCard } from "../components/index.js";
import { t } from "../i18n.js";
import { api } from "../utils/api.js";

export default defineComponent({
  name: "TopicsView",
  components: {AsyncState, TopicCard},
  setup() {
    const error = ref("");
    const loading = ref(true);
    const topics = ref([]);

    async function load() {
      loading.value = true;
      try {
        topics.value = await api("api/topics") || [];
        error.value = "";
      } catch (cause) {
        error.value = cause.message || t("error.fetchTopics");
      } finally {
        loading.value = false;
      }
    }

    async function kick(topic) {
      if (!window.confirm(t("confirm.kickTopic", {name: topic.name}))) return;
      try {
        await api(`api/topics/${encodeURIComponent(topic.name)}/kick`, {method: "POST"});
        await load();
      } catch (cause) {
        window.alert(t("common.error", {message: cause.message}));
      }
    }

    onMounted(load);
    return {error, kick, load, loading, topics};
  },
  template: `
    <AsyncState v-if="loading || error" :loading="loading" :error="error" :loading-label="$t('topics.loading')" :error-title="$t('topics.loadError')" @retry="load" />
    <div v-else>
      <header class="shadow-sm border-b bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
        <div class="flex justify-between items-center h-16">
          <div class="flex items-center space-x-4"><span class="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</span><span class="text-gray-400 dark:text-[#6e7681]">/</span><span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('nav.topics') }}</span></div>
          <button type="button" class="accent-solid px-4 py-2 rounded text-sm" @click="load">{{ $t('common.refresh') }}</button>
        </div>
      </header>
      <main class="py-8">
        <div class="mb-8"><h1 class="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('nav.topics') }}</h1><p class="text-gray-600 dark:text-[#8b949e]">{{ $t('topics.description') }}</p></div>
        <div v-if="topics.length" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"><TopicCard v-for="topic in topics" :key="topic.name" :topic="topic" @kick="kick" /></div>
        <div v-else class="rounded-lg p-12 text-center bg-white dark:bg-[#161b22] text-gray-500 dark:text-[#8b949e]"><div class="text-lg font-medium">{{ $t('topics.emptyTitle') }}</div><p class="mt-1">{{ $t('topics.emptyDescription') }}</p></div>
      </main>
    </div>
  `,
});
