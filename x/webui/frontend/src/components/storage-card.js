import { defineComponent } from "vue";

export default defineComponent({
  name: "StorageCard",
  props: {
    description: {type: String, required: true},
    icon: {type: String, required: true},
    stats: {type: Array, default: () => []},
    title: {type: String, required: true},
  },
  template: `
    <div class="p-6 rounded-lg shadow-sm border transition-shadow bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d] hover:shadow-md dark:hover:bg-[#1c2128]">
      <div class="flex items-center mb-4">
        <div class="accent-icon w-10 h-10 rounded-lg flex items-center justify-center mr-3">{{ icon }}</div>
        <div><h3 class="text-lg font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ title }}</h3><p class="text-sm text-gray-500 dark:text-[#8b949e]">{{ description }}</p></div>
      </div>
      <div class="grid grid-cols-2 gap-4">
        <div
          v-for="(stat, index) in stats"
          :key="stat.label"
          class="text-left"
          :style="stats.length % 2 === 1 && index === stats.length - 1 ? {gridColumn: '1 / -1'} : undefined"
        ><div class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ stat.value }}</div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ stat.label }}</div></div>
      </div>
    </div>
  `,
});
