import { defineComponent } from "vue";

import AppLink from "../components/app-link.js";

export default defineComponent({
  name: "NotFoundView",
  components: {AppLink},
  template: `
    <section class="flex items-center justify-center min-h-96 text-center">
      <div><div class="accent-text text-7xl font-bold">404</div><h1 class="mt-4 text-2xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('notFound.title') }}</h1><p class="mt-2 text-gray-600 dark:text-[#8b949e]">{{ $t('notFound.description') }}</p><AppLink class="accent-solid inline-block mt-6 px-4 py-2 rounded">{{ $t('common.backToDashboard') }}</AppLink></div>
    </section>
  `,
});
