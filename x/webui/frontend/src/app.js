import {
  computed,
  defineComponent,
  onBeforeUnmount,
  onErrorCaptured,
  onMounted,
  ref,
} from "vue";

import { AppLayout } from "./components/index.js";
import { installRouter, route } from "./router.js";
import { installPreferences } from "./stores/preferences.js";
import {
  DashboardView,
  JobDetailView,
  MetricsView,
  NotFoundView,
  SettingsView,
  StorageView,
  TopicDetailView,
  TopicsView,
} from "./views/index.js";

const views = {
  dashboard: DashboardView,
  job: JobDetailView,
  metrics: MetricsView,
  "not-found": NotFoundView,
  settings: SettingsView,
  storage: StorageView,
  topic: TopicDetailView,
  topics: TopicsView,
};

export default defineComponent({
  name: "App",
  components: {AppLayout},
  setup() {
    const renderError = ref("");
    const currentView = computed(() => views[route.name] || NotFoundView);
    const viewProps = computed(() => {
      if (route.name === "topic") return {topic: route.topic};
      if (route.name === "job") return {id: route.id};
      return {};
    });
    let removePreferences = () => {};
    let removeRouter = () => {};

    onMounted(() => {
      removePreferences = installPreferences();
      removeRouter = installRouter();
    });
    onBeforeUnmount(() => {
      removePreferences();
      removeRouter();
    });
    onErrorCaptured((error) => {
      renderError.value = error instanceof Error ? error.message : String(error);
      return false;
    });

    return {currentView, renderError, route, viewProps};
  },
  template: `
    <AppLayout>
      <div v-if="renderError" class="flex items-center justify-center min-h-96 text-center"><div><div class="text-red-600 text-4xl mb-4">&#9888;</div><h2 class="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('error.application') }}</h2><p class="text-gray-600 dark:text-[#8b949e]">{{ renderError }}</p></div></div>
      <component v-else :is="currentView" :key="route.key" v-bind="viewProps" />
    </AppLayout>
  `,
});
