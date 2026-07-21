import { defineComponent } from "vue";

import AppLink from "../components/app-link.js";
import LanguageSection from "../components/settings/language-section.js";
import PollingIntervalSection from "../components/settings/polling-interval-section.js";
import SidebarSection from "../components/settings/sidebar-section.js";
import ThemePreferencesSection from "../components/settings/theme-preferences-section.js";
import { resetPreferences } from "../stores/preferences.js";

export default defineComponent({
  name: "SettingsView",
  components: {
    AppLink,
    LanguageSection,
    PollingIntervalSection,
    SidebarSection,
    ThemePreferencesSection,
  },
  setup() {
    return {resetPreferences};
  },
  template: `
    <div class="flex justify-between items-center h-16 mb-8">
      <div class="flex items-center space-x-4"><AppLink class="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">{{ $t('nav.dashboard') }}</AppLink><span class="text-gray-400 dark:text-[#6e7681]">/</span><span class="font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ $t('settings.title') }}</span></div>
      <button type="button" class="px-4 py-2 rounded text-sm transition-colors bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]" @click="resetPreferences">{{ $t('settings.reset') }}</button>
    </div>
    <div class="mb-8"><h1 class="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">{{ $t('settings.title') }}</h1><p class="text-gray-600 dark:text-[#8b949e]">{{ $t('settings.description') }}</p></div>
    <ThemePreferencesSection />
    <LanguageSection />
    <PollingIntervalSection />
    <SidebarSection />
  `,
});
