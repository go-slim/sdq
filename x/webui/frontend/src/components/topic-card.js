import { defineComponent } from "vue";

import AppLink from "./app-link.js";
import StateBadge from "./state-badge.js";

export default defineComponent({
  name: "TopicCard",
  components: {AppLink, StateBadge},
  props: {
    topic: {type: Object, required: true},
  },
  emits: ["kick"],
  template: `
    <AppLink :to="'topics/' + encodeURIComponent(topic.name)" class="block p-6 rounded-lg shadow-sm border transition-shadow bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d] hover:shadow-md dark:hover:bg-[#1c2128]">
      <div class="flex justify-between items-start mb-4">
        <h3 class="text-lg font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ topic.name }}</h3>
        <StateBadge state="ready" />
      </div>
      <div class="grid grid-cols-2 gap-4 mb-4">
        <div class="text-center"><div class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ $formatNumber(topic.total_jobs) }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('common.total') }}</div></div>
        <div class="text-center"><div class="text-2xl font-bold text-green-600 dark:text-[#3fb950]">{{ $formatNumber(topic.ready_jobs) }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('common.ready') }}</div></div>
        <div class="text-center"><div class="text-2xl font-bold text-blue-600 dark:text-[#58a6ff]">{{ $formatNumber(topic.reserved_jobs) }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('common.reserved') }}</div></div>
        <div class="text-center"><div class="text-2xl font-bold text-yellow-600 dark:text-[#d29922]">{{ $formatNumber(topic.delayed_jobs) }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('common.delayed') }}</div></div>
      </div>
      <div v-if="topic.buried_jobs > 0" class="flex justify-between items-center">
        <div class="text-center"><div class="text-2xl font-bold text-red-600 dark:text-[#f85149]">{{ $formatNumber(topic.buried_jobs) }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ $t('common.buried') }}</div></div>
        <button type="button" class="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600" @click.prevent.stop="$emit('kick', topic)">{{ $t('dashboard.kickAll') }}</button>
      </div>
    </AppLink>
  `,
});
