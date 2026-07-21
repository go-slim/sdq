import { defineComponent } from "vue";

import SettingsSection from "../settings-section.js";
import { activeLanguage, setLanguage } from "../../stores/preferences.js";

const options = [
  {labelKey: "settings.english", value: "en"},
  {labelKey: "settings.chinese", value: "zh"},
];

export default defineComponent({
  name: "LanguageSection",
  components: {SettingsSection},
  setup() {
    return {activeLanguage, options, setLanguage};
  },
  template: `
    <SettingsSection :title="$t('settings.language')" icon="🌐">
      <div class="py-4"><label class="block text-sm font-medium mb-3 text-gray-700 dark:text-[#c9d1d9]">{{ $t('settings.selectLanguage') }}</label><div class="grid grid-cols-2 gap-3 max-w-xs">
        <button v-for="option in options" :key="option.value" type="button" :aria-pressed="activeLanguage === option.value" :class="['p-4 border-2 rounded-lg text-center transition-all hover:scale-105', activeLanguage === option.value ? 'accent-surface accent-border shadow-sm' : 'border-gray-200 dark:border-[#30363d] hover:border-gray-300 dark:hover:border-[#484f58] text-gray-600 dark:text-[#8b949e]']" @click="setLanguage(option.value)"><div class="text-sm font-medium">{{ $t(option.labelKey) }}</div></button>
      </div></div>
    </SettingsSection>
  `,
});
