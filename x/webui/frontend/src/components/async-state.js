import { defineComponent } from "vue";

export default defineComponent({
  name: "AsyncState",
  props: {
    empty: {type: Boolean, default: false},
    emptyDescription: {type: String, default: ""},
    emptyTitle: {type: String, default: ""},
    error: {type: String, default: ""},
    errorTitle: {type: String, default: ""},
    loading: {type: Boolean, default: false},
    loadingLabel: {type: String, default: ""},
    tall: {type: Boolean, default: false},
  },
  emits: ["retry"],
  template: `
    <div :class="['flex items-center justify-center bg-gray-50 dark:bg-[#0d1117]', tall ? 'min-h-screen' : 'min-h-96']">
      <div v-if="loading" class="text-center">
        <div class="accent-spinner w-12 h-12 border-4 rounded-full animate-spin mx-auto mb-4"></div>
        <p class="text-gray-600 dark:text-[#8b949e]">{{ loadingLabel || $t('async.loading') }}</p>
      </div>
      <div v-else-if="empty" class="text-center">
        <div class="text-4xl mb-4 text-gray-600 dark:text-[#8b949e]">&#128229;</div>
        <h2 class="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ emptyTitle || $t('async.noData') }}</h2>
        <p v-if="emptyDescription" class="text-gray-600 dark:text-[#8b949e]">{{ emptyDescription }}</p>
      </div>
      <div v-else class="text-center">
        <div class="text-red-600 text-4xl mb-4">&#9888;</div>
        <h2 class="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ errorTitle || $t('async.loadError') }}</h2>
        <p class="mb-4 text-gray-600 dark:text-[#8b949e]">{{ error }}</p>
        <button type="button" class="accent-solid px-4 py-2 rounded" @click="$emit('retry')">
          {{ $t('common.retry') }}
        </button>
        <slot />
      </div>
    </div>
  `,
});
