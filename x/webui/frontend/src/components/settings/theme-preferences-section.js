import { computed, defineComponent } from "vue";

import { t } from "../../i18n.js";
import { preferences, setTheme } from "../../stores/preferences.js";
import ThemePreviewWrapper from "./theme-preview-wrapper.js";

const themeOptions = [
  {descriptionKey: "settings.themeSystemDescription", labelKey: "settings.themeSystem", value: "system"},
  {descriptionKey: "settings.themeLightDescription", labelKey: "settings.themeLight", value: "light"},
  {descriptionKey: "settings.themeDarkDescription", labelKey: "settings.themeDark", value: "dark"},
];

export default defineComponent({
  name: "ThemePreferencesSection",
  components: {ThemePreviewWrapper},
  setup() {
    const description = computed(() => {
      const option = themeOptions.find((item) => item.value === preferences.theme);
      return option ? t(option.descriptionKey) : "";
    });
    return {description, preferences, setTheme, themeOptions};
  },
  template: `
    <div class="p-8 rounded-lg border border-[#30363d] mb-6">
      <h2 class="text-2xl font-semibold mb-2">{{ $t('settings.themePreferences') }}</h2>
      <p class="text-sm mb-6 leading-relaxed max-w-4xl">{{ $t('settings.themeDescription') }}</p>
      <div class="mb-8"><label class="block font-semibold mb-3">{{ $t('settings.themeMode') }}</label><select :value="preferences.theme" class="accent-focus border border-gray-300 dark:border-gray-700 rounded-md px-4 py-2 pr-10 appearance-none cursor-pointer w-80" style="background-image: url(&quot;data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3E%3Cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='m6 8 4 4 4-4'/%3E%3C/svg%3E&quot;); background-position: right 0.5rem center; background-repeat: no-repeat; background-size: 1.5em 1.5em" @change="setTheme($event.target.value)"><option v-for="option in themeOptions" :key="option.value" :value="option.value">{{ $t(option.labelKey) }}</option></select><p class="text-[#8b949e] text-sm mt-2">{{ description }}</p></div>
      <div class="grid grid-cols-2 gap-8"><ThemePreviewWrapper :dark="false" /><ThemePreviewWrapper dark /></div>
    </div>
  `,
});
