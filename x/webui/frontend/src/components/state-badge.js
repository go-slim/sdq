import { computed, defineComponent } from "vue";

import { t } from "../i18n.js";

const classes = {
  ready: "bg-green-100 text-green-800 dark:bg-[#1b2e1f] dark:text-[#3fb950]",
  delayed: "bg-yellow-100 text-yellow-800 dark:bg-[#341a00] dark:text-[#d29922]",
  reserved: "bg-blue-100 text-blue-800 dark:bg-[#1c2d41] dark:text-[#58a6ff]",
  buried: "bg-red-100 text-red-800 dark:bg-[#2e1a1f] dark:text-[#f85149]",
  enqueued: "bg-gray-100 text-gray-800 dark:bg-[#21262d] dark:text-[#c9d1d9]",
};

export default defineComponent({
  name: "StateBadge",
  props: {
    state: {type: String, default: "enqueued"},
  },
  setup(props) {
    const stateClass = computed(() => classes[props.state] || classes.enqueued);
    const stateLabel = computed(() => {
      const key = `state.${props.state}`;
      const label = t(key);
      return label === key ? props.state : label;
    });
    return {stateClass, stateLabel};
  },
  template: `
    <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium', stateClass]">
      {{ stateLabel }}
    </span>
  `,
});
