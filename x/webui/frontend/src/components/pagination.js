import { computed, defineComponent } from "vue";

export default defineComponent({
  name: "Pagination",
  props: {
    page: {type: Number, default: 1},
    totalPages: {type: Number, default: 1},
  },
  emits: ["change"],
  setup(props, {emit}) {
    const pages = computed(() => {
      const maxVisible = 5;
      let start = Math.max(1, props.page - Math.floor(maxVisible / 2));
      const end = Math.min(props.totalPages, start + maxVisible - 1);
      if (end - start < maxVisible - 1) start = Math.max(1, end - maxVisible + 1);
      return Array.from({length: Math.max(0, end - start + 1)}, (_, index) => start + index);
    });
    const buttonClass = (disabled, active = false) => {
      if (active) return "accent-solid";
      if (disabled) return "bg-gray-100 text-gray-400 cursor-not-allowed dark:bg-[#21262d] dark:text-[#6e7681]";
      return "bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]";
    };
    const go = (page) => emit("change", Math.min(props.totalPages, Math.max(1, page)));
    return {buttonClass, go, pages};
  },
  template: `
    <nav v-if="totalPages > 1" class="flex justify-center items-center mt-6 space-x-2" :aria-label="$t('common.pagination')">
      <button type="button" :disabled="page === 1" :class="['px-3 py-2 text-sm rounded', buttonClass(page === 1)]" @click="go(1)">&laquo;</button>
      <button type="button" :disabled="page === 1" :class="['px-3 py-2 text-sm rounded', buttonClass(page === 1)]" @click="go(page - 1)">&lsaquo;</button>
      <button
        v-for="item in pages"
        :key="item"
        type="button"
        :class="['px-3 py-2 text-sm rounded', buttonClass(false, item === page)]"
        @click="go(item)"
      >{{ item }}</button>
      <button type="button" :disabled="page === totalPages" :class="['px-3 py-2 text-sm rounded', buttonClass(page === totalPages)]" @click="go(page + 1)">&rsaquo;</button>
      <button type="button" :disabled="page === totalPages" :class="['px-3 py-2 text-sm rounded', buttonClass(page === totalPages)]" @click="go(totalPages)">&raquo;</button>
    </nav>
  `,
});
