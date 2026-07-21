import { computed, defineComponent } from "vue";

import { formatNumber } from "../i18n.js";

const colorClasses = {
  blue: "bg-blue-50 dark:bg-[#1c2d41] text-blue-700 dark:text-[#58a6ff] border-blue-200 dark:border-[#30363d]",
  gray: "bg-gray-50 dark:bg-[#21262d] text-gray-700 dark:text-[#c9d1d9] border-gray-200 dark:border-[#30363d]",
  green: "bg-green-50 dark:bg-[#1b2e1f] text-green-700 dark:text-[#3fb950] border-green-200 dark:border-[#30363d]",
  purple: "bg-purple-50 dark:bg-[#271c3a] text-purple-700 dark:text-[#b392f0] border-purple-200 dark:border-[#30363d]",
  red: "bg-red-50 dark:bg-[#2e1a1f] text-red-700 dark:text-[#f85149] border-red-200 dark:border-[#30363d]",
  yellow: "bg-yellow-50 dark:bg-[#341a00] text-yellow-700 dark:text-[#d29922] border-yellow-200 dark:border-[#30363d]",
};

export default defineComponent({
  name: "MetricCard",
  props: {
    color: {type: String, default: "blue"},
    icon: {type: String, required: true},
    title: {type: String, required: true},
    value: {type: [Number, String], required: true},
  },
  setup(props) {
    const cardClass = computed(() => colorClasses[props.color] || colorClasses.blue);
    const displayValue = computed(() => typeof props.value === "number" ? formatNumber(props.value) : props.value);
    return {cardClass, displayValue};
  },
  template: `
    <div :class="['bg-white dark:bg-[#161b22] p-6 rounded-lg shadow-sm border', cardClass]">
      <div class="flex items-center justify-between">
        <div class="flex items-center"><div class="w-10 h-10 rounded-lg flex items-center justify-center mr-3 bg-white dark:bg-[#0d1117]">{{ icon }}</div><div><div class="text-sm text-gray-500 dark:text-[#8b949e]">{{ title }}</div><div class="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{{ displayValue }}</div></div></div>
      </div>
    </div>
  `,
});
