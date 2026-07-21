import { computed, defineComponent } from "vue";

import { formatNumber } from "../i18n.js";

const colorClasses = {
  blue: "bg-blue-50 dark:bg-[#1c2d41] text-blue-700 dark:text-[#58a6ff] border-gray-200 dark:border-[#30363d]",
  green: "bg-green-50 dark:bg-[#1b2e1f] text-green-700 dark:text-[#3fb950] border-gray-200 dark:border-[#30363d]",
  yellow: "bg-yellow-50 dark:bg-[#341a00] text-yellow-700 dark:text-[#d29922] border-gray-200 dark:border-[#30363d]",
  red: "bg-red-50 dark:bg-[#2e1a1f] text-red-700 dark:text-[#f85149] border-gray-200 dark:border-[#30363d]",
  gray: "bg-gray-50 dark:bg-[#21262d] text-gray-700 dark:text-[#c9d1d9] border-gray-200 dark:border-[#30363d]",
};

export default defineComponent({
  name: "StatCard",
  props: {
    color: {type: String, default: "blue"},
    label: {type: String, required: true},
    value: {type: [Number, String], default: 0},
  },
  setup(props) {
    const cardClass = computed(() => colorClasses[props.color] || colorClasses.blue);
    const displayValue = computed(() => typeof props.value === "number" ? formatNumber(props.value) : props.value);
    return {cardClass, displayValue};
  },
  template: `
    <div :class="[cardClass, 'p-6 rounded-lg border']">
      <h3 class="text-lg font-semibold mb-2">{{ label }}</h3>
      <p class="text-3xl font-bold">{{ displayValue }}</p>
    </div>
  `,
});
